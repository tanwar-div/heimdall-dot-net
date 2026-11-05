using Heimdall.Shared.Cluster;
using Heimdall.Shared.Http;

namespace Heimdall.Gateway.Endpoints;

/// <summary>
/// Read-only views of the routing layer. These exist because consistent hashing is otherwise
/// invisible: without them you cannot tell whether a video went to the node you expected, or
/// whether the ring is actually balanced.
/// </summary>
public static class ClusterEndpoints
{
    public static void MapClusterEndpoints(this WebApplication app)
    {
        var cluster = app.MapGroup("/api/cluster");

        cluster.MapGet("/", (ClusterTopology topology) =>
        {
            var shares = topology.Ring.OwnershipShares();
            return Results.Ok(new
            {
                nodeCount = topology.Ring.NodeCount,
                tokenCount = topology.Ring.TokenCount,
                virtualNodesPerNode = topology.Ring.VirtualNodesPerNode,
                replicationFactor = topology.ReplicationFactor,
                readReplicaPercentage = topology.LoadBalancer.ReadReplicaPercentage,
                nodes = topology.Ring.Nodes
                    .OrderBy(static n => n.Id, StringComparer.Ordinal)
                    .Select(n => new
                    {
                        n.Id,
                        n.BaseUrl,
                        n.Weight,
                        keyspaceShare = Math.Round(shares.GetValueOrDefault(n.Id), 6),
                    }),
            });
        });

        // Where would this id land? Answers without touching any node, because placement is pure.
        cluster.MapGet("/placement/{videoId}", (string videoId, ClusterTopology topology) =>
        {
            var placement = topology.GetPlacement(videoId);
            return Results.Ok(new
            {
                videoId,
                ringHash = ConsistentHashRing.HashKey(videoId).ToString("x16"),
                primary = placement.Primary.Id,
                replicas = placement.Replicas.Select(static n => n.Id),
                readCandidates = placement.ReadCandidates.Select(static n => n.Id),
                replicasSelectedForReads = topology.LoadBalancer.ReplicaSelectionSize(placement.Replicas.Count),
            });
        });

        cluster.MapGet("/health", async (
            ClusterTopology topology,
            StorageNodeClient client,
            CancellationToken cancellationToken) =>
        {
            var probes = topology.Ring.Nodes
                .OrderBy(static n => n.Id, StringComparer.Ordinal)
                .Select(async node => new
                {
                    node.Id,
                    node.BaseUrl,
                    healthy = await client.PingAsync(node, cancellationToken),
                });

            var results = await Task.WhenAll(probes);
            var healthy = results.Count(static r => r.healthy);

            return Results.Json(new
            {
                healthyNodes = healthy,
                totalNodes = results.Length,
                nodes = results,
            },
            statusCode: healthy == 0 ? StatusCodes.Status503ServiceUnavailable : StatusCodes.Status200OK);
        });

        // Hash a synthetic key set and report the spread. This is the endpoint to point a
        // distribution benchmark at: it exercises the real ring, not a model of it.
        cluster.MapGet("/distribution", (ClusterTopology topology, int keys = 10_000) =>
        {
            if (keys is < 1 or > 1_000_000)
                return Results.BadRequest(new { error = "keys must be between 1 and 1,000,000." });

            var counts = topology.Ring.Nodes.ToDictionary(
                static n => n.Id, static _ => 0, StringComparer.Ordinal);

            for (var i = 0; i < keys; i++)
                counts[topology.Ring.GetPrimary($"bench-key-{i}").Id]++;

            var ideal = keys / (double)counts.Count;
            var maxDeviation = counts.Values.Max(c => Math.Abs(c - ideal)) / ideal;

            return Results.Ok(new
            {
                keys,
                nodes = counts.Count,
                idealPerNode = Math.Round(ideal, 2),
                maxDeviationFromIdeal = Math.Round(maxDeviation, 4),
                primaryCounts = counts.OrderBy(static kv => kv.Key, StringComparer.Ordinal)
                    .ToDictionary(static kv => kv.Key, static kv => kv.Value),
            });
        });
    }
}
