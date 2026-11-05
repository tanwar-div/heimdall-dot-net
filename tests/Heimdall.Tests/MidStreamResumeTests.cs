using Heimdall.Gateway;
using Heimdall.Gateway.Proxy;
using Heimdall.Shared.Cluster;
using Heimdall.Shared.Contracts;
using Heimdall.Shared.Http;
using Heimdall.Shared.Streaming;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Heimdall.Tests;

/// <summary>
/// The hardest failure to handle: a node that dies *after* the response headers have gone out.
/// There is no way to change the status code or Content-Length at that point, so the only correct
/// behaviour is to deliver the remaining bytes from somewhere else.
/// </summary>
public class MidStreamResumeTests
{
    private const string VideoId = "clip-under-test";

    private static (VideoStreamProxy Proxy, VideoPlacement Placement) Build(FakeNodeHandler handler)
    {
        var client = new StorageNodeClient(
            new HttpClient(handler), NullLogger<StorageNodeClient>.Instance, apiKey: "test-key");

        var proxy = new VideoStreamProxy(
            TestCluster.Topology(nodeCount: 6, replicationFactor: 3, readReplicaPercentage: 60),
            client,
            Options.Create(new GatewayOptions { ProxyBufferBytes = 4096 }),
            NullLogger<VideoStreamProxy>.Instance);

        // A twin topology, used once, yields the placement the proxy is about to choose.
        var expected = TestCluster
            .Topology(nodeCount: 6, replicationFactor: 3, readReplicaPercentage: 60)
            .GetPlacement(VideoId);

        return (proxy, expected);
    }

    private static (DefaultHttpContext Context, MemoryStream Body) Request(string? range = null)
    {
        var context = new DefaultHttpContext();
        var body = new MemoryStream();
        context.Response.Body = body;
        context.Request.Method = "GET";
        if (range is not null) context.Request.Headers.Range = range;
        return (context, body);
    }

    private static byte[] Payload(int length)
    {
        var bytes = new byte[length];
        new Random(99).NextBytes(bytes);
        return bytes;
    }

    [Fact]
    public async Task A_node_that_dies_mid_body_is_resumed_from_the_next_holder()
    {
        var payload = Payload(60_000);
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);

        // First candidate sends 20,000 of the 60,000 bytes, then its connection drops.
        var breaking = placement.ReadCandidates[0];
        handler.TruncatesAfter(breaking.Id, payload, afterBytes: 20_000);
        foreach (var node in placement.ReadCandidates.Skip(1)) handler.Serves(node.Id, payload);

        var (context, body) = Request();
        var outcome = await proxy.StreamAsync(context, VideoId, headOnly: false, CancellationToken.None);

        Assert.Equal(ProxyResult.Served, outcome.Result);
        Assert.True(outcome.Complete, "the response should have been completed from another holder");
        Assert.True(outcome.WasResumed);
        Assert.Equal([placement.ReadCandidates[1].Id], outcome.ResumedOnNodeIds);

        // The client's body is the whole file, byte for byte, with no duplicated or missing region
        // at the seam.
        Assert.Equal(60_000, body.Length);
        Assert.Equal(payload, body.ToArray());
        Assert.Equal(60_000, outcome.BytesRelayed);

        // The headers the client saw were the original node's, and they were never revised.
        Assert.Equal(StatusCodes.Status200OK, context.Response.StatusCode);
        Assert.Equal(60_000, context.Response.ContentLength);
        Assert.Equal(breaking.Id, context.Response.Headers[HeimdallHeaders.ServedByNode]);
    }

    [Fact]
    public async Task The_resume_asks_for_the_absolute_offset_it_stopped_at()
    {
        var payload = Payload(50_000);
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);

        handler.TruncatesAfter(placement.ReadCandidates[0].Id, payload, afterBytes: 12_000);
        foreach (var node in placement.ReadCandidates.Skip(1)) handler.Serves(node.Id, payload);

        var (context, body) = Request();
        await proxy.StreamAsync(context, VideoId, headOnly: false, CancellationToken.None);

        // The copy loop reads in 4 KiB buffers, so it stops on a buffer boundary at or below the
        // break point - whatever that is, the resume must name exactly that absolute offset.
        var delivered = body.Length;
        Assert.InRange(delivered, 50_000, 50_000);

        var resumeRequest = handler.RangeHeaders.Last();
        Assert.StartsWith("bytes=", resumeRequest);
        Assert.EndsWith("-49999", resumeRequest);
        Assert.Equal(payload, body.ToArray());
    }

    [Fact]
    public async Task A_broken_range_request_resumes_within_the_requested_window_not_the_whole_file()
    {
        // The subtle case: the client asked for bytes 10,000-39,999. A resume must re-request an
        // absolute sub-range of *that window*, or the response would be stitched from the wrong
        // part of the file.
        var payload = Payload(100_000);
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);

        handler.TruncatesAfter(placement.ReadCandidates[0].Id, payload, afterBytes: 8_000);
        foreach (var node in placement.ReadCandidates.Skip(1)) handler.Serves(node.Id, payload);

        var (context, body) = Request(range: "bytes=10000-39999");
        var outcome = await proxy.StreamAsync(context, VideoId, headOnly: false, CancellationToken.None);

        Assert.Equal(StatusCodes.Status206PartialContent, context.Response.StatusCode);
        Assert.Equal("bytes 10000-39999/100000", context.Response.Headers.ContentRange);
        Assert.Equal(30_000, context.Response.ContentLength);

        Assert.True(outcome.Complete);
        Assert.True(outcome.WasResumed);
        Assert.Equal(payload.AsSpan(10_000, 30_000).ToArray(), body.ToArray());

        // The resume ended at the window's last byte, not the file's.
        Assert.EndsWith("-39999", handler.RangeHeaders.Last());
    }

    [Fact]
    public async Task Resuming_walks_past_holders_that_cannot_help()
    {
        var payload = Payload(40_000);
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);

        handler.TruncatesAfter(placement.ReadCandidates[0].Id, payload, afterBytes: 9_000);
        handler.Unreachable(placement.ReadCandidates[1].Id);
        handler.Serves(placement.ReadCandidates[2].Id, payload);

        var (context, body) = Request();
        var outcome = await proxy.StreamAsync(context, VideoId, headOnly: false, CancellationToken.None);

        Assert.True(outcome.Complete);
        Assert.Equal([placement.ReadCandidates[2].Id], outcome.ResumedOnNodeIds);
        Assert.Equal(payload, body.ToArray());
    }

    [Fact]
    public async Task A_truncated_response_is_reported_when_no_holder_can_resume()
    {
        // Honest failure: the client gets a short body, and the gateway says so rather than
        // claiming success.
        var payload = Payload(40_000);
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);

        handler.TruncatesAfter(placement.ReadCandidates[0].Id, payload, afterBytes: 9_000);
        foreach (var node in placement.ReadCandidates.Skip(1)) handler.Unreachable(node.Id);

        var (context, body) = Request();
        var outcome = await proxy.StreamAsync(context, VideoId, headOnly: false, CancellationToken.None);

        Assert.False(outcome.Complete);
        Assert.True(body.Length < 40_000);
        Assert.Equal(payload.AsSpan(0, (int)body.Length).ToArray(), body.ToArray());
    }

    [Fact]
    public async Task A_holder_answering_a_resume_with_200_is_refused_rather_than_concatenated()
    {
        // A 200 would mean "here is the file from byte zero". Appending that to what has already
        // been sent would produce a corrupt body that still matched Content-Length, which is the
        // worst possible outcome - so it is rejected and the next holder is tried.
        var payload = Payload(30_000);
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);

        handler.TruncatesAfter(placement.ReadCandidates[0].Id, payload, afterBytes: 8_000);

        // This node ignores Range entirely and always replies 200 with the whole body.
        handler.Node(placement.ReadCandidates[1].Id, _ =>
        {
            var response = new HttpResponseMessage(System.Net.HttpStatusCode.OK)
            {
                Content = new ByteArrayContent(payload),
            };
            response.Content.Headers.ContentLength = payload.Length;
            return response;
        });

        handler.Serves(placement.ReadCandidates[2].Id, payload);

        var (context, body) = Request();
        var outcome = await proxy.StreamAsync(context, VideoId, headOnly: false, CancellationToken.None);

        Assert.True(outcome.Complete);
        Assert.Equal([placement.ReadCandidates[2].Id], outcome.ResumedOnNodeIds);
        Assert.Equal(payload, body.ToArray());
    }

    [Fact]
    public async Task A_clean_read_reports_no_resume()
    {
        var payload = Payload(20_000);
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);
        foreach (var node in placement.Holders) handler.Serves(node.Id, payload);

        var outcome = await proxy.StreamAsync(Request().Context, VideoId, headOnly: false, CancellationToken.None);

        Assert.True(outcome.Complete);
        Assert.False(outcome.WasResumed);
        Assert.Empty(outcome.ResumedOnNodeIds);
    }
}

public class ContentRangeTests
{
    [Fact]
    public void Parses_a_well_formed_header()
    {
        var parsed = ContentRange.Parse("bytes 1000-1999/30000000");

        Assert.True(parsed.Success);
        Assert.Equal(1000, parsed.From);
        Assert.Equal(1999, parsed.To);
        Assert.Equal(30_000_000, parsed.TotalLength);
    }

    [Fact]
    public void Handles_multi_gigabyte_offsets()
    {
        var parsed = ContentRange.Parse("bytes 5368709120-5368710143/6442450944");

        Assert.True(parsed.Success);
        Assert.Equal(5_368_709_120, parsed.From);
        Assert.Equal(1024, parsed.To - parsed.From + 1);
    }

    [Fact]
    public void An_unknown_total_still_yields_usable_offsets()
    {
        var parsed = ContentRange.Parse("bytes 0-99/*");

        Assert.True(parsed.Success);
        Assert.Equal(0, parsed.From);
        Assert.Equal(99, parsed.To);
        Assert.Equal(-1, parsed.TotalLength);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("bytes */30000000")]      // the 416 form: a length, but no satisfied range
    [InlineData("items 0-10/100")]
    [InlineData("bytes 0-10")]
    [InlineData("bytes abc-def/100")]
    [InlineData("bytes 100-50/200")]      // end before start
    public void Unusable_headers_report_failure_rather_than_guessing(string? header)
        => Assert.False(ContentRange.Parse(header).Success);

    [Fact]
    public void Round_trips_against_the_header_the_server_emits()
    {
        var range = new ByteRange(4096, 8191);
        var parsed = ContentRange.Parse(range.ToContentRange(1_000_000));

        Assert.True(parsed.Success);
        Assert.Equal(range.From, parsed.From);
        Assert.Equal(range.To, parsed.To);
        Assert.Equal(1_000_000, parsed.TotalLength);
    }
}
