using System.Net.Http.Json;
using Heimdall.Shared.Cluster;
using Heimdall.Shared.Contracts;
using Heimdall.Shared.Http;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Net.Http.Headers;

namespace Heimdall.Gateway.Routing;

public enum UploadStatus
{
    Stored,
    PrimaryUnavailable,
    PrimaryRejected,
    NoFilePart,
}

public sealed record UploadOutcome(
    UploadStatus Status,
    VideoManifest? Manifest,
    VideoPlacement Placement,
    string? Error,
    int? PrimaryStatusCode);

/// <summary>
/// Routes an upload to the node the ring says owns it and streams the client's body straight
/// through to that node.
///
/// Writes are deliberately primary-only. The upload body is a one-shot stream - once it has been
/// partly forwarded there is nothing left to retry with and nothing to hand to a second node - so
/// a request whose primary is down is failed with 503 and the id (and therefore the owner) stays
/// stable for the client's retry. Durability comes from the primary fanning the video out to
/// replicas afterwards, not from the gateway writing to several nodes at once.
/// </summary>
public sealed class VideoUploadService
{
    private readonly ClusterTopology _topology;
    private readonly StorageNodeClient _client;
    private readonly ILogger<VideoUploadService> _logger;

    public VideoUploadService(
        ClusterTopology topology,
        StorageNodeClient client,
        ILogger<VideoUploadService> logger)
    {
        _topology = topology;
        _client = client;
        _logger = logger;
    }

    public async Task<UploadOutcome> UploadAsync(
        HttpContext context,
        string videoId,
        CancellationToken cancellationToken)
    {
        var placement = _topology.GetPlacement(videoId);
        var primary = placement.Primary;

        var source = await OpenRequestBodyAsync(context, cancellationToken).ConfigureAwait(false);
        if (source is null)
        {
            return new UploadOutcome(
                UploadStatus.NoFilePart, null, placement,
                "multipart/form-data body contained no file part.", null);
        }

        _logger.LogInformation(
            "Routing upload of {VideoId} to primary {PrimaryId} (replicas: {Replicas}).",
            videoId, primary.Id, string.Join(", ", placement.Replicas.Select(static n => n.Id)));

        try
        {
            await using var body = source.Stream;

            using var response = await _client.PutVideoAsync(
                primary,
                videoId,
                body,
                source.ContentType,
                source.FileName,
                contentLength: source.Length,
                isReplicaWrite: false,
                ownerNodeId: primary.Id,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
            {
                var detail = await SafeReadAsync(response, cancellationToken).ConfigureAwait(false);
                _logger.LogError(
                    "Primary {PrimaryId} rejected upload of {VideoId}: {Status} {Detail}",
                    primary.Id, videoId, (int)response.StatusCode, detail);

                return new UploadOutcome(
                    UploadStatus.PrimaryRejected, null, placement, detail, (int)response.StatusCode);
            }

            var manifest = await response.Content.ReadFromJsonAsync<VideoManifest>(cancellationToken)
                .ConfigureAwait(false);

            return new UploadOutcome(UploadStatus.Stored, manifest, placement, null, (int)response.StatusCode);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex, "Primary {PrimaryId} is unreachable; upload of {VideoId} refused.", primary.Id, videoId);

            return new UploadOutcome(
                UploadStatus.PrimaryUnavailable, null, placement, ex.Message, null);
        }
    }

    private sealed record BodySource(Stream Stream, string ContentType, string? FileName, long? Length);

    /// <summary>
    /// Produces the stream to forward. Two shapes are accepted, and neither is buffered:
    /// a raw body (<c>curl --data-binary</c>, <c>fetch</c> with a Blob), or
    /// <c>multipart/form-data</c>, whose first file part is located with a streaming
    /// <see cref="MultipartReader"/> rather than by materialising the form.
    /// </summary>
    private async Task<BodySource?> OpenRequestBodyAsync(HttpContext context, CancellationToken cancellationToken)
    {
        var request = context.Request;
        var declaredName = request.Headers[HeimdallHeaders.FileName].ToString();
        if (string.IsNullOrWhiteSpace(declaredName))
            declaredName = request.Query["fileName"].ToString();

        var isMultipart = !string.IsNullOrEmpty(request.ContentType)
            && request.ContentType.Contains("multipart/form-data", StringComparison.OrdinalIgnoreCase);

        if (!isMultipart)
        {
            return new BodySource(
                request.Body,
                string.IsNullOrWhiteSpace(request.ContentType) ? "video/mp4" : request.ContentType,
                string.IsNullOrWhiteSpace(declaredName) ? null : declaredName,
                request.ContentLength);
        }

        var boundary = HeaderUtilities.RemoveQuotes(
            MediaTypeHeaderValue.Parse(request.ContentType!).Boundary).Value;

        if (string.IsNullOrEmpty(boundary)) return null;

        var reader = new MultipartReader(boundary, request.Body);

        for (var section = await reader.ReadNextSectionAsync(cancellationToken).ConfigureAwait(false);
             section is not null;
             section = await reader.ReadNextSectionAsync(cancellationToken).ConfigureAwait(false))
        {
            if (!ContentDispositionHeaderValue.TryParse(section.ContentDisposition, out var disposition))
                continue;

            var fileName = HeaderUtilities.RemoveQuotes(
                disposition.FileNameStar.HasValue ? disposition.FileNameStar : disposition.FileName).Value;

            if (string.IsNullOrEmpty(fileName)) continue;   // a plain form field, not the upload

            // section.Body stays valid until the next ReadNextSectionAsync call, which this loop
            // will not make: the stream is handed straight to the storage node.
            return new BodySource(
                section.Body,
                string.IsNullOrWhiteSpace(section.ContentType) ? "video/mp4" : section.ContentType!,
                string.IsNullOrWhiteSpace(declaredName) ? fileName : declaredName,
                // The part's length is not known without reading it, so the forwarded PUT uses
                // chunked transfer encoding.
                Length: null);
        }

        return null;
    }

    private static async Task<string> SafeReadAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        try
        {
            var text = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            return text.Length > 512 ? text[..512] : text;
        }
        catch
        {
            return response.ReasonPhrase ?? "unknown error";
        }
    }
}
