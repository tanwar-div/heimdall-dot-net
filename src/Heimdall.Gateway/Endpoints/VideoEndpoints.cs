using Heimdall.Gateway.Proxy;
using Heimdall.Gateway.Routing;
using Heimdall.Shared.Cluster;
using Heimdall.Shared.Contracts;
using Heimdall.Shared.Http;
using Heimdall.Shared.Security;
using Microsoft.AspNetCore.Http.Extensions;
using Microsoft.Extensions.Options;

namespace Heimdall.Gateway.Endpoints;

/// <summary>The gateway's client-facing video API.</summary>
public static class VideoEndpoints
{
    public static void MapVideoEndpoints(this WebApplication app)
    {
        var videos = app.MapGroup("/api/videos");

        // ---- upload -----------------------------------------------------------------------

        videos.MapPost("/", async (
            HttpContext context,
            VideoUploadService uploads,
            ApiKeyGuard guard,
            IOptions<GatewayOptions> gateway,
            CancellationToken cancellationToken) =>
        {
            if (!guard.TryAuthorize(context.Request, out var unauthorized))
                return unauthorized;

            var fileName = FileNameFrom(context);
            var videoId = VideoId.Create(fileName);
            return await StoreAsync(context, uploads, gateway.Value, videoId, cancellationToken);
        });

        // Explicit-id upload. Useful for idempotent re-uploads and for pinning a video to a known
        // ring position while testing placement.
        videos.MapPut("/{videoId}", async (
            string videoId,
            HttpContext context,
            VideoUploadService uploads,
            ApiKeyGuard guard,
            IOptions<GatewayOptions> gateway,
            CancellationToken cancellationToken) =>
        {
            if (!guard.TryAuthorize(context.Request, out var unauthorized))
                return unauthorized;

            if (!VideoId.IsValid(videoId))
                return Results.BadRequest(new { error = "videoId must be 1-128 characters of letters, digits, '-', '_' or '.'." });

            return await StoreAsync(context, uploads, gateway.Value, videoId, cancellationToken);
        });

        // ---- playback ---------------------------------------------------------------------

        videos.MapMethods("/{videoId}", ["GET", "HEAD"], async (
            string videoId,
            HttpContext context,
            VideoStreamProxy proxy,
            CancellationToken cancellationToken) =>
        {
            if (!VideoId.IsValid(videoId))
                return Results.BadRequest(new { error = "Malformed videoId." });

            var headOnly = HttpMethods.IsHead(context.Request.Method);
            var outcome = await proxy.StreamAsync(context, videoId, headOnly, cancellationToken);

            return outcome.Result switch
            {
                ProxyResult.NotFound => Results.NotFound(new { videoId, error = "No node holds this video." }),
                ProxyResult.NoHealthyNode => Results.Problem(
                    detail: $"None of the nodes holding '{videoId}' could be reached. Tried: {string.Join(", ", outcome.SkippedNodeIds)}.",
                    statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "No healthy holder"),
                _ => Results.Empty,   // headers and body already written by the proxy
            };
        });

        videos.MapGet("/{videoId}/manifest", async (
            string videoId,
            ClusterTopology topology,
            StorageNodeClient client,
            CancellationToken cancellationToken) =>
        {
            var placement = topology.GetPlacement(videoId);

            // Ask holders in read order and take the first answer, so a stale replica does not
            // hide a video the primary has.
            foreach (var candidate in placement.ReadCandidates)
            {
                try
                {
                    var manifest = await client.GetManifestAsync(candidate, videoId, cancellationToken);
                    if (manifest is not null)
                    {
                        return Results.Ok(new
                        {
                            manifest,
                            placement = new
                            {
                                primary = placement.Primary.Id,
                                replicas = placement.Replicas.Select(static n => n.Id),
                            },
                            servedBy = candidate.Id,
                        });
                    }
                }
                catch (Exception) when (!cancellationToken.IsCancellationRequested)
                {
                    // Unreachable node: try the next holder.
                }
            }

            return Results.NotFound(new { videoId, error = "No reachable node holds this video." });
        });

        // ---- delete -----------------------------------------------------------------------

        videos.MapDelete("/{videoId}", async (
            string videoId,
            HttpContext context,
            ClusterTopology topology,
            StorageNodeClient client,
            ApiKeyGuard guard,
            CancellationToken cancellationToken) =>
        {
            if (!guard.TryAuthorize(context.Request, out var unauthorized))
                return unauthorized;

            var placement = topology.GetPlacement(videoId);

            // Every holder is told to drop its copy. A node that is down keeps its copy and will
            // serve a deleted video until it is swept; that is the cost of having no coordinator.
            var results = await Task.WhenAll(placement.Holders.Select(async node =>
            {
                try
                {
                    return new { node = node.Id, deleted = await client.DeleteVideoAsync(node, videoId, cancellationToken) };
                }
                catch (Exception)
                {
                    return new { node = node.Id, deleted = false };
                }
            }));

            return Results.Ok(new { videoId, holders = results });
        });

        // ---- index ------------------------------------------------------------------------

        app.MapGet("/api", (HttpContext context, ClusterTopology topology) => Results.Ok(new
        {
            service = "heimdall-gateway",
            self = context.Request.GetDisplayUrl(),
            cluster = new { nodes = topology.Ring.NodeCount, replicationFactor = topology.ReplicationFactor },
            endpoints = new[]
            {
                "POST   /api/videos                      (X-Heimdall-Key; raw body or multipart)",
                "PUT    /api/videos/{videoId}            (X-Heimdall-Key)",
                "GET    /api/videos/{videoId}            (Range -> 206 Partial Content)",
                "HEAD   /api/videos/{videoId}",
                "GET    /api/videos/{videoId}/manifest",
                "DELETE /api/videos/{videoId}            (X-Heimdall-Key)",
                "GET    /api/cluster",
                "GET    /api/cluster/health",
                "GET    /api/cluster/placement/{videoId}",
                "GET    /api/cluster/distribution?keys=10000",
            },
        }));
    }

    private static async Task<IResult> StoreAsync(
        HttpContext context,
        VideoUploadService uploads,
        GatewayOptions gateway,
        string videoId,
        CancellationToken cancellationToken)
    {
        var outcome = await uploads.UploadAsync(context, videoId, cancellationToken);

        context.Response.Headers[HeimdallHeaders.PrimaryNode] = outcome.Placement.Primary.Id;

        return outcome.Status switch
        {
            UploadStatus.Stored when outcome.Manifest is not null => Results.Created(
                $"/api/videos/{Uri.EscapeDataString(videoId)}",
                new UploadResult
                {
                    VideoId = videoId,
                    Length = outcome.Manifest.Length,
                    ContentHash = outcome.Manifest.ContentHash,
                    PrimaryNodeId = outcome.Placement.Primary.Id,
                    ReplicaNodeIds = outcome.Placement.Replicas.Select(static n => n.Id).ToArray(),
                    StreamUrl = BuildStreamUrl(context, gateway, videoId),
                }),

            UploadStatus.NoFilePart => Results.BadRequest(new { error = outcome.Error }),

            UploadStatus.PrimaryUnavailable => Results.Problem(
                detail: $"Primary node '{outcome.Placement.Primary.Id}' is unreachable. " +
                        "The upload body cannot be replayed to another node, so retry the request - " +
                        "the id hashes to the same owner, and it will be accepted once that node is back.",
                statusCode: StatusCodes.Status503ServiceUnavailable,
                title: "Primary unavailable"),

            _ => Results.Problem(
                detail: outcome.Error ?? "The primary node rejected the upload.",
                statusCode: outcome.PrimaryStatusCode ?? StatusCodes.Status502BadGateway,
                title: "Upload rejected"),
        };
    }

    private static string? FileNameFrom(HttpContext context)
    {
        var header = context.Request.Headers[HeimdallHeaders.FileName].ToString();
        if (!string.IsNullOrWhiteSpace(header)) return header;

        var query = context.Request.Query["fileName"].ToString();
        return string.IsNullOrWhiteSpace(query) ? null : query;
    }

    private static string BuildStreamUrl(HttpContext context, GatewayOptions gateway, string videoId)
    {
        var path = $"/api/videos/{Uri.EscapeDataString(videoId)}";
        if (!string.IsNullOrWhiteSpace(gateway.PublicBaseUrl))
            return gateway.PublicBaseUrl.TrimEnd('/') + path;

        return $"{context.Request.Scheme}://{context.Request.Host}{path}";
    }
}
