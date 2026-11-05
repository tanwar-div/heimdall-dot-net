using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using Heimdall.Shared.Cluster;
using Heimdall.Shared.Contracts;
using Microsoft.Extensions.Logging;

namespace Heimdall.Shared.Http;

/// <summary>
/// Typed client for a storage node's internal API. Used by the gateway to proxy client traffic and
/// by a primary node to push replicas.
///
/// Every method streams. Reads use <see cref="HttpCompletionOption.ResponseHeadersRead"/> so the
/// caller gets headers back as soon as the node has them and can decide whether to fail over
/// *before* any bytes are copied; writes hand the node a <see cref="StreamContent"/> so the body
/// flows through without being materialised.
/// </summary>
public sealed class StorageNodeClient
{
    public const string HttpClientName = "heimdall-storage-node";

    private readonly HttpClient _http;
    private readonly ILogger<StorageNodeClient> _logger;
    private readonly string? _apiKey;

    public StorageNodeClient(HttpClient http, ILogger<StorageNodeClient> logger, string? apiKey)
    {
        _http = http;
        _logger = logger;
        _apiKey = apiKey;
    }

    /// <summary>Is the node answering at all? Used by health checks, not on the read path.</summary>
    public async Task<bool> PingAsync(NodeDescriptor node, CancellationToken cancellationToken = default)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, new Uri(node.BaseUri, StorageNodeRoutes.Health));
            using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
                .ConfigureAwait(false);
            return response.IsSuccessStatusCode;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogDebug(ex, "Health probe of {NodeId} failed.", node.Id);
            return false;
        }
    }

    /// <summary>Manifest for a video on a specific node, or null when that node does not hold it.</summary>
    public async Task<VideoManifest?> GetManifestAsync(
        NodeDescriptor node,
        string videoId,
        CancellationToken cancellationToken = default)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, new Uri(node.BaseUri, StorageNodeRoutes.Manifest(videoId)));
        using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);

        if (response.StatusCode == HttpStatusCode.NotFound) return null;
        response.EnsureSuccessStatusCode();

        return await response.Content.ReadFromJsonAsync<VideoManifest>(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Opens a read against <paramref name="node"/>. The returned response is live and must be
    /// disposed by the caller; its content stream is the node's chunk stream, not a buffer.
    /// </summary>
    public Task<HttpResponseMessage> OpenVideoStreamAsync(
        NodeDescriptor node,
        string videoId,
        string? rangeHeader,
        bool headOnly = false,
        CancellationToken cancellationToken = default)
    {
        var request = new HttpRequestMessage(
            headOnly ? HttpMethod.Head : HttpMethod.Get,
            new Uri(node.BaseUri, StorageNodeRoutes.Video(videoId)));

        if (!string.IsNullOrWhiteSpace(rangeHeader))
            request.Headers.TryAddWithoutValidation("Range", rangeHeader);

        return SendAndDisposeRequestAsync(request, cancellationToken);
    }

    /// <summary>
    /// Streams <paramref name="content"/> to a node as the body of a PUT. When
    /// <paramref name="contentLength"/> is null the request is sent with chunked transfer encoding,
    /// which is what happens when a client uploads without a Content-Length.
    /// </summary>
    public Task<HttpResponseMessage> PutVideoAsync(
        NodeDescriptor node,
        string videoId,
        Stream content,
        string contentType,
        string? fileName = null,
        long? contentLength = null,
        bool isReplicaWrite = false,
        string? ownerNodeId = null,
        CancellationToken cancellationToken = default)
    {
        var request = new HttpRequestMessage(HttpMethod.Put, new Uri(node.BaseUri, StorageNodeRoutes.Video(videoId)))
        {
            Content = new StreamContent(content),
        };

        request.Content.Headers.ContentType = MediaTypeHeaderValue.TryParse(contentType, out var parsed)
            ? parsed
            : new MediaTypeHeaderValue("application/octet-stream");

        // Setting ContentLength to null is what makes HttpClient fall back to chunked encoding.
        request.Content.Headers.ContentLength = contentLength;

        ApplyApiKey(request);

        if (!string.IsNullOrWhiteSpace(fileName))
            request.Headers.TryAddWithoutValidation(HeimdallHeaders.FileName, fileName);
        if (isReplicaWrite)
            request.Headers.TryAddWithoutValidation(HeimdallHeaders.ReplicaWrite, "1");
        if (!string.IsNullOrWhiteSpace(ownerNodeId))
            request.Headers.TryAddWithoutValidation(HeimdallHeaders.OwnerNode, ownerNodeId);

        return SendAndDisposeRequestAsync(request, cancellationToken);
    }

    public async Task<bool> DeleteVideoAsync(
        NodeDescriptor node,
        string videoId,
        CancellationToken cancellationToken = default)
    {
        using var request = new HttpRequestMessage(HttpMethod.Delete, new Uri(node.BaseUri, StorageNodeRoutes.Video(videoId)));
        ApplyApiKey(request);

        using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);

        return response.IsSuccessStatusCode || response.StatusCode == HttpStatusCode.NotFound;
    }

    private void ApplyApiKey(HttpRequestMessage request)
    {
        if (!string.IsNullOrEmpty(_apiKey))
            request.Headers.TryAddWithoutValidation(HeimdallHeaders.ApiKey, _apiKey);
    }

    /// <summary>
    /// Sends the request and disposes the <see cref="HttpRequestMessage"/> once headers are back,
    /// while leaving the response (and its still-open content stream) to the caller.
    /// </summary>
    private async Task<HttpResponseMessage> SendAndDisposeRequestAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        try
        {
            return await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            request.Dispose();
        }
    }
}
