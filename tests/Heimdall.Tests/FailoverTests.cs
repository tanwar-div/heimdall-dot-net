using Heimdall.Gateway;
using Heimdall.Gateway.Proxy;
using Heimdall.Shared.Cluster;
using Heimdall.Shared.Contracts;
using Heimdall.Shared.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Heimdall.Tests;

public class FailoverTests
{
    private const string VideoId = "clip-under-test";

    /// <summary>
    /// Builds the proxy together with the placement it is going to choose.
    ///
    /// The load balancer rotates its replica selection on every call, so the expected placement is
    /// read from a *twin* topology built with identical configuration rather than from the proxy's
    /// own. Both start at rotation 0, so as long as each topology is asked for a placement exactly
    /// once - the proxy does so per request, these tests issue one request - the two agree. That
    /// keeps the assertions exact without having to freeze the rotation that makes load balancing
    /// work in the first place.
    /// </summary>
    private static (VideoStreamProxy Proxy, VideoPlacement Placement) Build(FakeNodeHandler handler)
    {
        var client = new StorageNodeClient(
            new HttpClient(handler),
            NullLogger<StorageNodeClient>.Instance,
            apiKey: "test-key");

        var proxy = new VideoStreamProxy(
            TestCluster.Topology(nodeCount: 6, replicationFactor: 3, readReplicaPercentage: 60),
            client,
            Options.Create(new GatewayOptions()),
            NullLogger<VideoStreamProxy>.Instance);

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
        new Random(7).NextBytes(bytes);
        return bytes;
    }

    [Fact]
    public async Task Reads_are_served_by_a_replica_not_the_primary()
    {
        // With ReadReplicaPercentage = 60 the load balancer puts a replica at the front of the
        // chain, so a healthy cluster answers reads without touching the owning node.
        var payload = Payload(4096);
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);

        foreach (var node in placement.Holders) handler.Serves(node.Id, payload);

        var (context, body) = Request();
        var outcome = await proxy.StreamAsync(context, VideoId, headOnly: false, CancellationToken.None);

        Assert.Equal(ProxyResult.Served, outcome.Result);
        Assert.Equal(payload, body.ToArray());
        Assert.Equal(placement.ReadCandidates[0].Id, outcome.ServedByNodeId);
        Assert.NotEqual(placement.Primary.Id, outcome.ServedByNodeId);
        Assert.Equal(placement.Primary.Id, context.Response.Headers[HeimdallHeaders.PrimaryNode]);
    }

    [Fact]
    public async Task A_dead_first_candidate_is_skipped_and_the_next_holder_serves_the_bytes()
    {
        // The headline failover case: kill the node the read was routed to, and the client still
        // gets a complete, correct response.
        var payload = Payload(8192);
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);

        var dead = placement.ReadCandidates[0];
        handler.Unreachable(dead.Id);
        foreach (var node in placement.ReadCandidates.Skip(1)) handler.Serves(node.Id, payload);

        var (context, body) = Request();
        var outcome = await proxy.StreamAsync(context, VideoId, headOnly: false, CancellationToken.None);

        Assert.Equal(ProxyResult.Served, outcome.Result);
        Assert.Equal(payload, body.ToArray());
        Assert.Equal(placement.ReadCandidates[1].Id, outcome.ServedByNodeId);
        Assert.Equal([dead.Id], outcome.SkippedNodeIds);
        Assert.Equal(dead.Id, context.Response.Headers[HeimdallHeaders.FailedOverFrom]);
        Assert.Equal(outcome.ServedByNodeId, context.Response.Headers[HeimdallHeaders.ServedByNode]);
    }

    [Fact]
    public async Task Failover_walks_the_whole_chain_including_the_primary()
    {
        var payload = Payload(2048);
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);

        // Only the last holder in the read order survives.
        foreach (var node in placement.ReadCandidates.Take(placement.ReadCandidates.Count - 1))
            handler.Unreachable(node.Id);

        var survivor = placement.ReadCandidates[^1];
        handler.Serves(survivor.Id, payload);

        var (context, body) = Request();
        var outcome = await proxy.StreamAsync(context, VideoId, headOnly: false, CancellationToken.None);

        Assert.Equal(ProxyResult.Served, outcome.Result);
        Assert.Equal(survivor.Id, outcome.ServedByNodeId);
        Assert.Equal(placement.ReadCandidates.Count - 1, outcome.SkippedNodeIds.Count);
        Assert.Equal(payload, body.ToArray());
    }

    [Fact]
    public async Task A_replica_that_has_not_caught_up_yet_is_skipped_rather_than_reported_as_missing()
    {
        // Replication is asynchronous, so a read can arrive at a replica before the copy does.
        // That must not surface as a 404 while the primary still has the video.
        var payload = Payload(1024);
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);

        foreach (var node in placement.Replicas) handler.Missing(node.Id);
        handler.Serves(placement.Primary.Id, payload);

        var (context, body) = Request();
        var outcome = await proxy.StreamAsync(context, VideoId, headOnly: false, CancellationToken.None);

        Assert.Equal(ProxyResult.Served, outcome.Result);
        Assert.Equal(placement.Primary.Id, outcome.ServedByNodeId);
        Assert.Equal(payload, body.ToArray());
    }

    [Fact]
    public async Task A_broken_node_returning_500_is_failed_over_just_like_a_dead_one()
    {
        var payload = Payload(1024);
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);

        handler.Failing(placement.ReadCandidates[0].Id);
        foreach (var node in placement.ReadCandidates.Skip(1)) handler.Serves(node.Id, payload);

        var outcome = await proxy.StreamAsync(Request().Context, VideoId, headOnly: false, CancellationToken.None);

        Assert.Equal(ProxyResult.Served, outcome.Result);
        Assert.Equal(placement.ReadCandidates[1].Id, outcome.ServedByNodeId);
    }

    [Fact]
    public async Task A_video_no_holder_has_is_a_404()
    {
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);

        foreach (var node in placement.Holders) handler.Missing(node.Id);

        var outcome = await proxy.StreamAsync(Request().Context, VideoId, headOnly: false, CancellationToken.None);

        Assert.Equal(ProxyResult.NotFound, outcome.Result);
        Assert.Null(outcome.ServedByNodeId);
    }

    [Fact]
    public async Task An_entirely_unreachable_holder_set_is_a_503_not_a_404()
    {
        // The distinction matters: "we cannot reach anyone" is a cluster problem, while 404 would
        // tell the client the video is gone.
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);

        foreach (var node in placement.Holders) handler.Unreachable(node.Id);

        var outcome = await proxy.StreamAsync(Request().Context, VideoId, headOnly: false, CancellationToken.None);

        Assert.Equal(ProxyResult.NoHealthyNode, outcome.Result);
        Assert.Equal(placement.ReadCandidates.Count, outcome.SkippedNodeIds.Count);
    }

    [Fact]
    public async Task A_404_mixed_with_an_unreachable_node_is_still_a_503()
    {
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);

        handler.Missing(placement.ReadCandidates[0].Id);
        foreach (var node in placement.ReadCandidates.Skip(1)) handler.Unreachable(node.Id);

        var outcome = await proxy.StreamAsync(Request().Context, VideoId, headOnly: false, CancellationToken.None);

        // One of the nodes we could not reach might be the one holding it.
        Assert.Equal(ProxyResult.NoHealthyNode, outcome.Result);
    }

    [Fact]
    public async Task Range_requests_pass_through_as_206_with_the_right_headers_and_bytes()
    {
        var payload = Payload(10_000);
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);

        foreach (var node in placement.Holders) handler.Serves(node.Id, payload);

        var (context, body) = Request(range: "bytes=1000-1999");
        var outcome = await proxy.StreamAsync(context, VideoId, headOnly: false, CancellationToken.None);

        Assert.Equal(ProxyResult.Served, outcome.Result);
        Assert.Equal(StatusCodes.Status206PartialContent, context.Response.StatusCode);
        Assert.Equal("bytes 1000-1999/10000", context.Response.Headers.ContentRange);
        Assert.Equal(1000, context.Response.ContentLength);
        Assert.Equal("bytes", context.Response.Headers.AcceptRanges);
        Assert.Equal(payload.AsSpan(1000, 1000).ToArray(), body.ToArray());
        Assert.Equal(1000, outcome.BytesRelayed);
    }

    [Fact]
    public async Task A_range_request_also_fails_over_and_still_returns_the_correct_slice()
    {
        // Failing over mid-seek is the case most likely to return subtly wrong bytes, so the slice
        // is compared exactly.
        var payload = Payload(10_000);
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);

        handler.Unreachable(placement.ReadCandidates[0].Id);
        foreach (var node in placement.ReadCandidates.Skip(1)) handler.Serves(node.Id, payload);

        var (context, body) = Request(range: "bytes=9500-");
        var outcome = await proxy.StreamAsync(context, VideoId, headOnly: false, CancellationToken.None);

        Assert.Equal(StatusCodes.Status206PartialContent, context.Response.StatusCode);
        Assert.Equal("bytes 9500-9999/10000", context.Response.Headers.ContentRange);
        Assert.Equal(payload.AsSpan(9500, 500).ToArray(), body.ToArray());
        Assert.Single(outcome.SkippedNodeIds);
    }

    [Fact]
    public async Task An_out_of_bounds_range_returns_416_without_trying_other_nodes()
    {
        // Every holder would give the same answer, so failing over would only add latency.
        var payload = Payload(1_000);
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);

        foreach (var node in placement.Holders) handler.Serves(node.Id, payload);

        var (context, _) = Request(range: "bytes=5000-6000");
        var outcome = await proxy.StreamAsync(context, VideoId, headOnly: false, CancellationToken.None);

        Assert.Equal(ProxyResult.RangeNotSatisfiable, outcome.Result);
        Assert.Equal(StatusCodes.Status416RangeNotSatisfiable, context.Response.StatusCode);
        Assert.Equal("bytes */1000", context.Response.Headers.ContentRange);
        Assert.Single(handler.Requests);
    }

    [Fact]
    public async Task HEAD_returns_the_headers_with_no_body()
    {
        var payload = Payload(5_000);
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);

        foreach (var node in placement.Holders) handler.Serves(node.Id, payload);

        var context = new DefaultHttpContext();
        var body = new MemoryStream();
        context.Response.Body = body;
        context.Request.Method = "HEAD";

        var outcome = await proxy.StreamAsync(context, VideoId, headOnly: true, CancellationToken.None);

        Assert.Equal(ProxyResult.Served, outcome.Result);
        Assert.Empty(body.ToArray());
        Assert.Equal(0, outcome.BytesRelayed);
        Assert.Contains("HEAD", handler.Requests[0]);
    }

    [Fact]
    public async Task The_read_candidate_chain_is_published_on_the_response()
    {
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);
        foreach (var node in placement.Holders) handler.Serves(node.Id, Payload(16));

        var (context, _) = Request();
        await proxy.StreamAsync(context, VideoId, headOnly: false, CancellationToken.None);

        Assert.Equal(
            string.Join(',', placement.ReadCandidates.Select(n => n.Id)),
            context.Response.Headers[HeimdallHeaders.ReadCandidates]);
    }

    [Fact]
    public async Task Candidates_are_tried_in_the_load_balancers_order()
    {
        var handler = new FakeNodeHandler();
        var (proxy, placement) = Build(handler);

        foreach (var node in placement.Holders) handler.Unreachable(node.Id);

        await proxy.StreamAsync(Request().Context, VideoId, headOnly: false, CancellationToken.None);

        Assert.Equal(
            placement.ReadCandidates.Select(n => n.Id),
            handler.Requests.Select(r => r.Split(' ')[0]));
    }
}
