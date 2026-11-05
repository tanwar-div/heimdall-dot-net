using System.Buffers;
using System.Net;
using Heimdall.Shared.Cluster;
using Heimdall.Shared.Contracts;
using Heimdall.Shared.Http;
using Heimdall.Shared.Streaming;
using Microsoft.Extensions.Options;
using Microsoft.Net.Http.Headers;

namespace Heimdall.Gateway.Proxy;

public enum ProxyResult
{
    Served,
    NotFound,
    NoHealthyNode,
    RangeNotSatisfiable,
}

public sealed record ProxyOutcome(
    ProxyResult Result,
    string? ServedByNodeId,
    IReadOnlyList<string> SkippedNodeIds,
    long BytesRelayed)
{
    /// <summary>Nodes that took over mid-body after an earlier one broke, in the order they did.</summary>
    public IReadOnlyList<string> ResumedOnNodeIds { get; init; } = Array.Empty<string>();

    /// <summary>True when the client's single response was stitched together from more than one node.</summary>
    public bool WasResumed => ResumedOnNodeIds.Count > 0;

    /// <summary>
    /// False when the response ended short of what its <c>Content-Length</c> promised - every
    /// holder broke and none could supply the remainder.
    /// </summary>
    public bool Complete { get; init; } = true;
}

/// <summary>
/// Relays a client's read to the nodes that hold the video, with failover before *and* during the
/// body transfer.
///
/// Read order comes from <see cref="ReplicaLoadBalancer"/>, so most requests are answered by a
/// replica and the owning node is left for writes. Candidates are tried until one returns usable
/// headers; nothing is written to the client until then, because a status line cannot be unsent.
///
/// Once bytes are flowing, a node that dies does not truncate the response: the gateway works out
/// the absolute file offset it has reached and asks the next holder for the remainder as a range
/// request, continuing into the same response body. The client sees one complete stream.
///
/// Nothing is buffered. Memory per in-flight request is one relay buffer, whether the client is
/// pulling a 2 KB moov atom or a 4 GB file.
/// </summary>
public sealed class VideoStreamProxy
{
    private static readonly string[] RelayedHeaders =
    [
        HeaderNames.ContentType,
        HeaderNames.ContentRange,
        HeaderNames.AcceptRanges,
        HeaderNames.ETag,
        HeaderNames.LastModified,
        HeaderNames.ContentDisposition,
    ];

    private readonly ClusterTopology _topology;
    private readonly StorageNodeClient _client;
    private readonly GatewayOptions _options;
    private readonly ILogger<VideoStreamProxy> _logger;

    public VideoStreamProxy(
        ClusterTopology topology,
        StorageNodeClient client,
        IOptions<GatewayOptions> options,
        ILogger<VideoStreamProxy> logger)
    {
        _topology = topology;
        _client = client;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<ProxyOutcome> StreamAsync(
        HttpContext context,
        string videoId,
        bool headOnly,
        CancellationToken cancellationToken)
    {
        var placement = _topology.GetPlacement(videoId);
        var rangeHeader = context.Request.Headers[HeaderNames.Range].ToString();

        context.Response.Headers[HeimdallHeaders.PrimaryNode] = placement.Primary.Id;
        context.Response.Headers[HeimdallHeaders.ReadCandidates] =
            string.Join(',', placement.ReadCandidates.Select(static n => n.Id));

        var skipped = new List<string>();
        var sawNotFound = false;
        var sawUnusableNode = false;

        for (var index = 0; index < placement.ReadCandidates.Count; index++)
        {
            var candidate = placement.ReadCandidates[index];

            // One CTS linked to the client's connection, with a timer guarding only the wait for
            // headers. The timer is disarmed once headers arrive: the CTS must stay alive and
            // uncancelled while the body streams, because HttpClient ties the response stream's
            // lifetime to the token passed to SendAsync.
            using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            attemptCts.CancelAfter(_options.CandidateHeaderTimeout);

            HttpResponseMessage? response = null;
            try
            {
                response = await _client
                    .OpenVideoStreamAsync(candidate, videoId, rangeHeader, headOnly, attemptCts.Token)
                    .ConfigureAwait(false);

                attemptCts.CancelAfter(Timeout.InfiniteTimeSpan);

                if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    // This node does not hold the video - a replica that has not caught up, for
                    // instance. Try the next holder before concluding it does not exist anywhere.
                    sawNotFound = true;
                    skipped.Add(candidate.Id);
                    continue;
                }

                if ((int)response.StatusCode >= 500)
                {
                    _logger.LogWarning(
                        "Node {NodeId} returned {Status} for {VideoId}; failing over.",
                        candidate.Id, (int)response.StatusCode, videoId);
                    sawUnusableNode = true;
                    skipped.Add(candidate.Id);
                    continue;
                }

                if (response.StatusCode == HttpStatusCode.RequestedRangeNotSatisfiable)
                {
                    // The node has the video and the client's range really is out of bounds. Every
                    // other holder would say the same, so failing over would only add latency.
                    RelayHeaders(context, response, candidate, skipped);
                    return new ProxyOutcome(ProxyResult.RangeNotSatisfiable, candidate.Id, skipped, 0);
                }

                if (!response.IsSuccessStatusCode)
                {
                    sawUnusableNode = true;
                    skipped.Add(candidate.Id);
                    continue;
                }

                RelayHeaders(context, response, candidate, skipped);

                if (headOnly)
                    return new ProxyOutcome(ProxyResult.Served, candidate.Id, skipped, 0);

                // Candidates after this one stay in hand so a mid-body failure can be resumed
                // instead of truncating the response.
                var fallbacks = placement.ReadCandidates.Skip(index + 1).ToArray();

                var relay = await RelayBodyAsync(
                    context, response, candidate, fallbacks, videoId, cancellationToken).ConfigureAwait(false);

                return new ProxyOutcome(ProxyResult.Served, candidate.Id, skipped, relay.BytesRelayed)
                {
                    ResumedOnNodeIds = relay.ResumedOn,
                    Complete = relay.Complete,
                };
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;   // the client hung up; there is nothing to fail over to
            }
            catch (Exception ex)
            {
                // Connection refused, DNS failure, header timeout, reset mid-handshake: unreachable
                // as far as this request is concerned.
                sawUnusableNode = true;
                skipped.Add(candidate.Id);
                _logger.LogWarning(
                    ex, "Read of {VideoId} from {NodeId} failed; failing over to the next holder.",
                    videoId, candidate.Id);
            }
            finally
            {
                response?.Dispose();
            }
        }

        _logger.LogError("No holder could serve {VideoId}. Tried: {Candidates}.", videoId, string.Join(", ", skipped));

        // 404 from every holder, with none of them failing, is the only case in which the video can
        // be said not to exist. If even one node was unreachable the honest answer is 503 - its
        // copy might be the one that has the video.
        var result = sawNotFound && !sawUnusableNode ? ProxyResult.NotFound : ProxyResult.NoHealthyNode;
        return new ProxyOutcome(result, null, skipped, 0);
    }

    private static void RelayHeaders(
        HttpContext context,
        HttpResponseMessage response,
        NodeDescriptor servedBy,
        IReadOnlyList<string> skipped)
    {
        context.Response.StatusCode = (int)response.StatusCode;

        foreach (var header in RelayedHeaders)
        {
            if (response.Content.Headers.TryGetValues(header, out var contentValues))
                context.Response.Headers[header] = contentValues.ToArray();
            else if (response.Headers.TryGetValues(header, out var values))
                context.Response.Headers[header] = values.ToArray();
        }

        // Accept-Ranges is what tells a browser it may seek at all, so never let it get lost.
        if (!context.Response.Headers.ContainsKey(HeaderNames.AcceptRanges))
            context.Response.Headers[HeaderNames.AcceptRanges] = "bytes";

        if (response.Content.Headers.ContentLength is { } length)
            context.Response.ContentLength = length;

        context.Response.Headers[HeimdallHeaders.ServedByNode] = servedBy.Id;
        if (skipped.Count > 0)
            context.Response.Headers[HeimdallHeaders.FailedOverFrom] = string.Join(',', skipped);
    }

    private readonly record struct RelayResult(long BytesRelayed, IReadOnlyList<string> ResumedOn, bool Complete);

    private enum CopyOutcome
    {
        /// <summary>The upstream body ended normally.</summary>
        Finished,

        /// <summary>The upstream broke. Worth asking another holder for the remainder.</summary>
        UpstreamBroke,

        /// <summary>The client went away. There is nowhere to resume into.</summary>
        ClientGone,
    }

    /// <summary>
    /// Copies the node's body to the client and, when that node dies partway through, continues the
    /// same response from another holder.
    ///
    /// The status line and <c>Content-Length</c> are already on the wire and cannot be revised -
    /// but they do not need to be. What the client was promised is a specific absolute byte range,
    /// and any holder can supply the unsent remainder of it: the gateway re-requests
    /// <c>bytes={position}-{end}</c> from the next candidate and keeps writing into the same
    /// response. The client sees one uninterrupted body and never learns it came from two machines.
    ///
    /// This is why the absolute offsets are recovered from the upstream <c>Content-Range</c> rather
    /// than tracked as a count of bytes written: a resume has to name a position in the original
    /// file, not in this response.
    /// </summary>
    private async Task<RelayResult> RelayBodyAsync(
        HttpContext context,
        HttpResponseMessage first,
        NodeDescriptor servedBy,
        IReadOnlyList<NodeDescriptor> fallbacks,
        string videoId,
        CancellationToken cancellationToken)
    {
        var declaredLength = first.Content.Headers.ContentLength;

        var contentRange = ContentRange.Parse(
            first.Content.Headers.TryGetValues(HeaderNames.ContentRange, out var values)
                ? values.FirstOrDefault()
                : null);

        // A 200 response carries no Content-Range, and is by definition the whole object.
        var absoluteFrom = contentRange.Success ? contentRange.From : 0;
        var absoluteTo = contentRange.Success
            ? contentRange.To
            : (declaredLength is > 0 ? declaredLength.Value - 1 : -1);

        var resumedOn = new List<string>();
        long delivered = 0;
        var fallbackIndex = 0;

        var source = first;
        var ownsSource = false;         // the caller disposes `first`; resumed responses are ours
        var currentNode = servedBy;

        while (true)
        {
            var (outcome, written) = await CopyAsync(source, context.Response.Body, videoId, currentNode, cancellationToken)
                .ConfigureAwait(false);

            delivered += written;

            if (ownsSource)
            {
                source.Dispose();
                ownsSource = false;
            }

            var complete = declaredLength is null || delivered >= declaredLength.Value;

            if (outcome != CopyOutcome.UpstreamBroke || complete)
            {
                await FlushQuietlyAsync(context, cancellationToken).ConfigureAwait(false);
                return new RelayResult(delivered, resumedOn, complete);
            }

            // The upstream broke early. Find a holder that can pick up where it stopped.
            var resumed = false;
            while (fallbackIndex < fallbacks.Count && absoluteTo >= 0)
            {
                var next = fallbacks[fallbackIndex++];
                var resumeRange = new ByteRange(absoluteFrom + delivered, absoluteTo).ToRangeHeader();

                _logger.LogWarning(
                    "Relay of {VideoId} from {FailedNode} broke at byte {Delivered}; resuming {Range} from {NextNode}.",
                    videoId, currentNode.Id, delivered, resumeRange, next.Id);

                HttpResponseMessage? candidate = null;
                try
                {
                    candidate = await _client
                        .OpenVideoStreamAsync(next, videoId, resumeRange, headOnly: false, cancellationToken)
                        .ConfigureAwait(false);

                    // Only a 206 proves the node honoured the offset. A 200 would restart from byte
                    // zero and corrupt everything already delivered, so it is refused outright.
                    if (candidate.StatusCode != HttpStatusCode.PartialContent)
                    {
                        _logger.LogWarning(
                            "{NextNode} answered a resume for {VideoId} with {Status} instead of 206; skipping it.",
                            next.Id, videoId, (int)candidate.StatusCode);
                        candidate.Dispose();
                        continue;
                    }

                    source = candidate;
                    ownsSource = true;
                    currentNode = next;
                    resumedOn.Add(next.Id);
                    resumed = true;
                    candidate = null;       // ownership moved to `source`
                    break;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    candidate?.Dispose();
                    return new RelayResult(delivered, resumedOn, Complete: false);
                }
                catch (Exception ex)
                {
                    candidate?.Dispose();
                    _logger.LogWarning(ex, "Resume of {VideoId} from {NextNode} failed; trying the next holder.", videoId, next.Id);
                }
            }

            if (!resumed)
            {
                _logger.LogError(
                    "Relay of {VideoId} truncated at {Delivered}/{Expected} bytes: no holder could resume.",
                    videoId, delivered, declaredLength);

                await FlushQuietlyAsync(context, cancellationToken).ConfigureAwait(false);
                return new RelayResult(delivered, resumedOn, Complete: false);
            }
        }
    }

    /// <summary>
    /// Pumps one upstream body into the client, reporting which side failed if either does.
    /// A read/write loop rather than <see cref="Stream.CopyToAsync(Stream)"/>: the whole point is
    /// to tell "the node stopped sending" (resumable) from "the client stopped listening" (not).
    /// </summary>
    private async Task<(CopyOutcome Outcome, long Written)> CopyAsync(
        HttpResponseMessage source,
        Stream destination,
        string videoId,
        NodeDescriptor node,
        CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(_options.ProxyBufferBytes);
        long written = 0;

        try
        {
            Stream upstream;
            try
            {
                upstream = await source.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is IOException or HttpRequestException)
            {
                _logger.LogDebug(ex, "Could not open the body stream from {NodeId} for {VideoId}.", node.Id, videoId);
                return (CopyOutcome.UpstreamBroke, 0);
            }

            // One CTS for the whole pass, with its timer armed only around reads. A node that
            // vanishes without closing its socket would otherwise block this read forever.
            using var readCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            await using (upstream)
            {
                while (true)
                {
                    int read;
                    try
                    {
                        readCts.CancelAfter(_options.RelayIdleTimeout);
                        read = await upstream.ReadAsync(buffer, readCts.Token).ConfigureAwait(false);
                        readCts.CancelAfter(Timeout.InfiniteTimeSpan);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        // A seeking player aborts requests constantly. Expected, not an error.
                        _logger.LogDebug("Client aborted {VideoId} after {Written} bytes.", videoId, written);
                        return (CopyOutcome.ClientGone, written);
                    }
                    catch (OperationCanceledException)
                    {
                        // The idle timer fired: the node is gone or wedged. Resumable.
                        _logger.LogWarning(
                            "Node {NodeId} went silent for {Timeout} while relaying {VideoId} at byte {Written}.",
                            node.Id, _options.RelayIdleTimeout, videoId, written);
                        return (CopyOutcome.UpstreamBroke, written);
                    }
                    catch (Exception ex) when (ex is IOException or HttpRequestException)
                    {
                        _logger.LogDebug(ex, "Upstream {NodeId} broke while relaying {VideoId}.", node.Id, videoId);
                        return (CopyOutcome.UpstreamBroke, written);
                    }

                    if (read == 0)
                        return (CopyOutcome.Finished, written);

                    try
                    {
                        // Deliberately the request token, not readCts: a paused player blocks here
                        // for as long as it likes without being treated as a failure.
                        await destination.WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
                        written += read;
                    }
                    catch (Exception ex) when (ex is IOException or OperationCanceledException)
                    {
                        _logger.LogDebug(ex, "Client closed the connection for {VideoId} after {Written} bytes.", videoId, written);
                        return (CopyOutcome.ClientGone, written);
                    }
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async Task FlushQuietlyAsync(HttpContext context, CancellationToken cancellationToken)
    {
        try
        {
            await context.Response.Body.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or OperationCanceledException)
        {
            // The client closed the connection first. Nothing to report.
        }
    }
}
