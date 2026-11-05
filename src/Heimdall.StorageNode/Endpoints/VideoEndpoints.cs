using Heimdall.Shared.Cluster;
using Heimdall.Shared.Contracts;
using Heimdall.Shared.Http;
using Heimdall.Shared.Security;
using Heimdall.Shared.Streaming;
using Heimdall.StorageNode.Replication;
using Heimdall.StorageNode.Storage;
using Microsoft.AspNetCore.Http.Extensions;
using Microsoft.Extensions.Options;
using Microsoft.Net.Http.Headers;

namespace Heimdall.StorageNode.Endpoints;

/// <summary>
/// The storage node's internal API. It is "internal" in the sense that clients are expected to go
/// through the gateway, but the contract is a plain HTTP one: the gateway and a replicating peer
/// both speak exactly this.
/// </summary>
public static class VideoEndpoints
{
    public static void MapVideoEndpoints(this WebApplication app)
    {
        app.MapGet("/" + StorageNodeRoutes.Health, (IOptions<NodeOptions> node) => Results.Ok(new
        {
            status = "healthy",
            node = node.Value.Id,
            utc = DateTimeOffset.UtcNow,
        }));

        app.MapGet("/" + StorageNodeRoutes.Ready, async (
            IObjectStore store,
            IOptions<NodeOptions> node,
            IOptions<StorageOptions> storage,
            CancellationToken cancellationToken) =>
        {
            var reachable = await store.ProbeAsync(cancellationToken);
            return reachable
                ? Results.Ok(new { status = "ready", node = node.Value.Id, bucket = storage.Value.Bucket })
                : Results.Json(
                    new { status = "object-store-unreachable", node = node.Value.Id, bucket = storage.Value.Bucket },
                    statusCode: StatusCodes.Status503ServiceUnavailable);
        });

        app.MapGet("/" + StorageNodeRoutes.NodeInfo, (
            IOptions<NodeOptions> node,
            IOptions<StorageOptions> storage,
            ClusterTopology topology) =>
        {
            var shares = topology.Ring.OwnershipShares();
            return Results.Ok(new
            {
                id = node.Value.Id,
                chunkSizeBytes = node.Value.ChunkSizeBytes,
                replicationEnabled = node.Value.ReplicationEnabled,
                bucket = storage.Value.Bucket,
                objectStoreEndpoint = storage.Value.Endpoint,
                cluster = new
                {
                    nodeCount = topology.Ring.NodeCount,
                    tokenCount = topology.Ring.TokenCount,
                    virtualNodesPerNode = topology.Ring.VirtualNodesPerNode,
                    replicationFactor = topology.ReplicationFactor,
                },
                keyspaceShare = shares.TryGetValue(node.Value.Id, out var share) ? Math.Round(share, 6) : 0d,
            });
        });

        // ---- write path -------------------------------------------------------------------

        app.MapPut(StorageNodeRoutes.VideoTemplate, async (
            string videoId,
            HttpContext context,
            ChunkedVideoRepository repository,
            ReplicationQueue replication,
            ApiKeyGuard guard,
            IOptions<NodeOptions> node,
            ILogger<Program> logger,
            CancellationToken cancellationToken) =>
        {
            if (!guard.TryAuthorize(context.Request, out var unauthorized))
                return unauthorized;

            if (!IsValidVideoId(videoId))
                return Results.BadRequest(new { error = "videoId must be 1-128 characters of letters, digits, '-', '_' or '.'." });

            var isReplicaWrite = context.Request.Headers.ContainsKey(HeimdallHeaders.ReplicaWrite);
            var ownerNodeId = context.Request.Headers.TryGetValue(HeimdallHeaders.OwnerNode, out var owner)
                ? owner.ToString()
                : node.Value.Id;

            var fileName = context.Request.Headers.TryGetValue(HeimdallHeaders.FileName, out var name)
                ? name.ToString()
                : null;

            // The request body is read straight into chunks. It is never buffered to disk or to a
            // byte[], so a multi-gigabyte PUT costs one chunk of memory.
            var manifest = await repository.IngestAsync(
                new VideoIngestRequest
                {
                    VideoId = videoId,
                    ContentType = context.Request.ContentType ?? "application/octet-stream",
                    FileName = string.IsNullOrWhiteSpace(fileName) ? null : fileName,
                    OwnerNodeId = string.IsNullOrWhiteSpace(ownerNodeId) ? node.Value.Id : ownerNodeId,
                    IsReplica = isReplicaWrite,
                },
                context.Request.Body,
                cancellationToken);

            // Only a client upload triggers fan-out. Replication traffic must not re-replicate, or
            // the cluster would gossip the same video around the ring forever.
            if (!isReplicaWrite && node.Value.ReplicationEnabled)
            {
                replication.Enqueue(ReplicationJob.For(videoId));
                logger.LogDebug("Queued replication for {VideoId}.", videoId);
            }

            return Results.Created($"/internal/videos/{Uri.EscapeDataString(videoId)}", manifest);
        });

        app.MapDelete(StorageNodeRoutes.VideoTemplate, async (
            string videoId,
            HttpContext context,
            ChunkedVideoRepository repository,
            ApiKeyGuard guard,
            CancellationToken cancellationToken) =>
        {
            if (!guard.TryAuthorize(context.Request, out var unauthorized))
                return unauthorized;

            var deleted = await repository.DeleteAsync(videoId, cancellationToken);
            return deleted == 0
                ? Results.NotFound(new { videoId, error = "Not stored on this node." })
                : Results.Ok(new { videoId, objectsDeleted = deleted });
        });

        // ---- read path --------------------------------------------------------------------

        app.MapGet(StorageNodeRoutes.ManifestTemplate, async (
            string videoId,
            ChunkedVideoRepository repository,
            CancellationToken cancellationToken) =>
        {
            var manifest = await repository.GetManifestAsync(videoId, cancellationToken);
            return manifest is null
                ? Results.NotFound(new { videoId, error = "Not stored on this node." })
                : Results.Ok(manifest);
        });

        app.MapMethods(StorageNodeRoutes.VideoTemplate, ["GET", "HEAD"], async (
            string videoId,
            HttpContext context,
            ChunkedVideoRepository repository,
            IOptions<NodeOptions> node,
            ILogger<Program> logger,
            CancellationToken cancellationToken) =>
        {
            var manifest = await repository.GetManifestAsync(videoId, cancellationToken);
            if (manifest is null)
                return Results.NotFound(new { videoId, error = "Not stored on this node." });

            var response = context.Response;
            response.Headers[HeaderNames.AcceptRanges] = "bytes";
            response.Headers[HeimdallHeaders.ServedByNode] = node.Value.Id;
            response.Headers[HeaderNames.ETag] = manifest.ETag;
            response.Headers[HeaderNames.LastModified] = manifest.CreatedAt.ToString("R");
            response.ContentType = manifest.ContentType;

            if (!string.IsNullOrWhiteSpace(manifest.FileName))
            {
                response.Headers[HeaderNames.ContentDisposition] =
                    new ContentDispositionHeaderValue("inline") { FileNameStar = manifest.FileName }.ToString();
            }

            var parsed = RangeHeader.Parse(context.Request.Headers[HeaderNames.Range], manifest.Length);

            if (parsed.Status == RangeParseStatus.Unsatisfiable)
            {
                // 416 must advertise the real size so the client can retry with a sane range.
                response.Headers[HeaderNames.ContentRange] = $"bytes */{manifest.Length}";
                return Results.StatusCode(StatusCodes.Status416RangeNotSatisfiable);
            }

            var range = parsed.IsPartial ? parsed.Range : ByteRange.Full(manifest.Length);

            if (parsed.IsPartial)
            {
                response.StatusCode = StatusCodes.Status206PartialContent;
                response.Headers[HeaderNames.ContentRange] = range.ToContentRange(manifest.Length);
            }
            else
            {
                response.StatusCode = StatusCodes.Status200OK;
            }

            response.ContentLength = manifest.Length == 0 ? 0 : range.Length;

            // HEAD gets the identical header set with no body, which is how the gateway discovers
            // length and ETag without moving any bytes.
            if (HttpMethods.IsHead(context.Request.Method))
                return Results.Empty;

            if (manifest.Length == 0)
                return Results.Empty;

            try
            {
                await repository.CopyRangeToAsync(manifest, range, response.Body, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // A seeking player aborts in-flight requests constantly; this is normal, not an error.
                logger.LogDebug("Client aborted read of {VideoId} {Range}.", videoId, range);
            }

            return Results.Empty;
        });

        // ---- diagnostics ------------------------------------------------------------------

        app.MapGet("/internal/replication", (ReplicationQueue queue) => Results.Ok(queue.Snapshot()));

        app.MapGet("/internal/replication/{videoId}", (string videoId, ReplicationQueue queue)
            => Results.Ok(queue.SnapshotFor(videoId)));

        app.MapGet("/", (IOptions<NodeOptions> node, HttpContext context) => Results.Ok(new
        {
            service = "heimdall-storage-node",
            node = node.Value.Id,
            self = context.Request.GetDisplayUrl(),
            endpoints = new[]
            {
                "GET  /health",
                "GET  /ready",
                "GET  /internal/node",
                "PUT  /internal/videos/{videoId}        (X-Heimdall-Key)",
                "GET  /internal/videos/{videoId}        (Range supported)",
                "HEAD /internal/videos/{videoId}",
                "GET  /internal/videos/{videoId}/manifest",
                "DELETE /internal/videos/{videoId}      (X-Heimdall-Key)",
                "GET  /internal/replication",
            },
        }));
    }

    /// <summary>
    /// Video ids become object-storage key prefixes, so they are restricted to characters that
    /// cannot escape the prefix or confuse a key listing.
    /// </summary>
    internal static bool IsValidVideoId(string videoId)
    {
        if (string.IsNullOrEmpty(videoId) || videoId.Length > 128) return false;
        if (videoId is "." or "..") return false;

        foreach (var c in videoId)
        {
            var ok = char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.';
            if (!ok) return false;
        }

        return true;
    }
}
