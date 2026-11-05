using System.Net;
using Heimdall.Shared.Contracts;

namespace Heimdall.Tests;

/// <summary>
/// Stand-in for a cluster of storage nodes. Each node id is given a behaviour, so a test can make
/// one node refuse connections, another return 404, and a third serve bytes - which is exactly the
/// situation the gateway's failover path exists for.
/// </summary>
internal sealed class FakeNodeHandler : HttpMessageHandler
{
    private readonly Dictionary<string, Func<HttpRequestMessage, HttpResponseMessage>> _behaviours =
        new(StringComparer.Ordinal);

    /// <summary>Every request the handler saw, as "nodeId METHOD path".</summary>
    public List<string> Requests { get; } = [];

    /// <summary>The Range header of every request, in order. Empty string when there was none.</summary>
    public List<string> RangeHeaders { get; } = [];

    public FakeNodeHandler Node(string nodeId, Func<HttpRequestMessage, HttpResponseMessage> behaviour)
    {
        _behaviours[nodeId] = behaviour;
        return this;
    }

    /// <summary>Node is up and holds the video.</summary>
    public FakeNodeHandler Serves(string nodeId, byte[] content, string contentType = "video/mp4")
        => Node(nodeId, request => BuildRangeResponse(request, content, contentType, nodeId));

    /// <summary>Node is up but does not hold the video.</summary>
    public FakeNodeHandler Missing(string nodeId)
        => Node(nodeId, _ => new HttpResponseMessage(HttpStatusCode.NotFound) { Content = new StringContent("{}") });

    /// <summary>Node process is gone: the TCP connect fails.</summary>
    public FakeNodeHandler Unreachable(string nodeId)
        => Node(nodeId, _ => throw new HttpRequestException(
            $"Connection refused ({nodeId}).", null, HttpStatusCode.ServiceUnavailable));

    /// <summary>
    /// Node answers with correct headers but its connection dies after <paramref name="afterBytes"/>
    /// bytes of body - the case that truncates a response unless the gateway can resume elsewhere.
    /// </summary>
    public FakeNodeHandler TruncatesAfter(string nodeId, byte[] content, int afterBytes, string contentType = "video/mp4")
        => Node(nodeId, request =>
        {
            var response = BuildRangeResponse(request, content, contentType, nodeId);
            if (request.Method == HttpMethod.Head) return response;

            var full = response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult();
            var declared = response.Content.Headers.ContentLength;
            var contentRange = response.Content.Headers.TryGetValues("Content-Range", out var cr)
                ? cr.FirstOrDefault()
                : null;

            // Hand back a body that yields `afterBytes` and then throws, while keeping the headers
            // the node legitimately produced - including the full declared Content-Length.
            response.Content = new StreamContent(new BreakingStream(full, afterBytes));
            response.Content.Headers.ContentLength = declared;
            response.Content.Headers.TryAddWithoutValidation("Content-Type", contentType);
            if (contentRange is not null)
                response.Content.Headers.TryAddWithoutValidation("Content-Range", contentRange);

            return response;
        });

    /// <summary>Node is up but broken - disk gone, object store unreachable.</summary>
    public FakeNodeHandler Failing(string nodeId, HttpStatusCode status = HttpStatusCode.InternalServerError)
        => Node(nodeId, _ => new HttpResponseMessage(status) { Content = new StringContent("{}") });

    /// <summary>Node accepts a write and echoes back a manifest.</summary>
    public FakeNodeHandler AcceptsWrites(string nodeId, Action<byte[]>? onBody = null)
        => Node(nodeId, request =>
        {
            var body = request.Content!.ReadAsByteArrayAsync().GetAwaiter().GetResult();
            onBody?.Invoke(body);

            var manifest = new VideoManifest
            {
                VideoId = request.RequestUri!.Segments[^1].Trim('/'),
                Length = body.Length,
                ChunkSize = 8 * 1024 * 1024,
                ChunkCount = 1,
                ContentHash = Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(body)),
                OwnerNodeId = nodeId,
                StoredOnNodeId = nodeId,
            };

            return new HttpResponseMessage(HttpStatusCode.Created)
            {
                Content = System.Net.Http.Json.JsonContent.Create(manifest),
            };
        });

    /// <summary>Yields a prefix of the payload, then fails the way a dropped connection does.</summary>
    private sealed class BreakingStream(byte[] data, int breakAfter) : Stream
    {
        private int _position;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => data.Length;

        public override long Position
        {
            get => _position;
            set => throw new NotSupportedException();
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (_position >= breakAfter)
                throw new IOException("The response ended prematurely.");

            var take = Math.Min(Math.Min(buffer.Length, breakAfter - _position), data.Length - _position);
            if (take <= 0) throw new IOException("The response ended prematurely.");

            data.AsSpan(_position, take).CopyTo(buffer.Span);
            _position += take;
            return ValueTask.FromResult(take);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override int Read(byte[] buffer, int offset, int count)
            => ReadAsync(buffer.AsMemory(offset, count), CancellationToken.None).AsTask().GetAwaiter().GetResult();

        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var nodeId = request.RequestUri!.Host;
        Requests.Add($"{nodeId} {request.Method} {request.RequestUri.AbsolutePath}");
        RangeHeaders.Add(request.Headers.Range?.ToString() ?? string.Empty);

        if (!_behaviours.TryGetValue(nodeId, out var behaviour))
            throw new HttpRequestException($"No behaviour configured for node '{nodeId}'.");

        return Task.FromResult(behaviour(request));
    }

    /// <summary>
    /// A minimal but correct Range implementation, so the gateway's passthrough is exercised
    /// against real 200/206/416 responses rather than a simplification of them.
    /// </summary>
    private static HttpResponseMessage BuildRangeResponse(
        HttpRequestMessage request,
        byte[] content,
        string contentType,
        string nodeId)
    {
        var parsed = Shared.Streaming.RangeHeader.Parse(
            request.Headers.Range?.ToString(), content.Length);

        var response = new HttpResponseMessage();
        response.Headers.TryAddWithoutValidation("Accept-Ranges", "bytes");
        response.Headers.TryAddWithoutValidation(HeimdallHeaders.ServedByNode, nodeId);

        if (parsed.Status == Shared.Streaming.RangeParseStatus.Unsatisfiable)
        {
            response.StatusCode = HttpStatusCode.RequestedRangeNotSatisfiable;
            response.Content = new ByteArrayContent([]);
            response.Content.Headers.TryAddWithoutValidation("Content-Range", $"bytes */{content.Length}");
            return response;
        }

        var range = parsed.IsPartial
            ? parsed.Range
            : Shared.Streaming.ByteRange.Full(content.Length);

        var slice = content.AsSpan((int)range.From, (int)range.Length).ToArray();
        var isHead = request.Method == HttpMethod.Head;

        response.StatusCode = parsed.IsPartial ? HttpStatusCode.PartialContent : HttpStatusCode.OK;
        response.Content = new ByteArrayContent(isHead ? [] : slice);
        response.Content.Headers.TryAddWithoutValidation("Content-Type", contentType);
        response.Content.Headers.ContentLength = slice.Length;

        if (parsed.IsPartial)
            response.Content.Headers.TryAddWithoutValidation("Content-Range", range.ToContentRange(content.Length));

        return response;
    }
}
