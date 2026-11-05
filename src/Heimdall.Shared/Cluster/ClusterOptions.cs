using System.ComponentModel.DataAnnotations;

namespace Heimdall.Shared.Cluster;

/// <summary>
/// Cluster-wide configuration, bound from the <c>Cluster</c> configuration section. Every process
/// in the cluster (gateway and all storage nodes) is given the same section, which is what lets
/// each of them compute placement locally instead of asking a coordinator.
/// </summary>
public sealed class ClusterOptions
{
    public const string SectionName = "Cluster";

    /// <summary>Every node in the cluster, in any order. Ring construction sorts by token, not by list position.</summary>
    public List<NodeDescriptor> Nodes { get; set; } = [];

    /// <summary>
    /// Virtual nodes per unit of weight. Higher values smooth the keyspace distribution at the cost
    /// of a larger ring; see <see cref="ConsistentHashRing.DefaultVirtualNodesPerNode"/>.
    /// </summary>
    [Range(1, 10_000)]
    public int VirtualNodesPerNode { get; set; } = ConsistentHashRing.DefaultVirtualNodesPerNode;

    /// <summary>
    /// How many nodes hold each video: 1 primary plus <c>ReplicationFactor - 1</c> replicas. Clamped
    /// to the cluster size at runtime.
    /// </summary>
    [Range(1, 64)]
    public int ReplicationFactor { get; set; } = 3;

    /// <summary>
    /// Percentage of a key's replicas the load balancer puts ahead of the primary when serving
    /// reads. 0 sends every read to the primary first; 100 spreads reads across all replicas.
    /// </summary>
    [Range(0, 100)]
    public int ReadReplicaPercentage { get; set; } = 60;

    /// <summary>Shared secret required on write paths (<c>X-Heimdall-Key</c>). Writes are rejected when unset.</summary>
    public string? ApiKey { get; set; }

    /// <summary>Per-attempt timeout when the gateway or a replicating primary talks to a node.</summary>
    public TimeSpan NodeRequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

    public void Validate()
    {
        if (Nodes.Count == 0)
            throw new InvalidOperationException("Cluster:Nodes is empty - configure at least one storage node.");

        foreach (var node in Nodes)
        {
            if (string.IsNullOrWhiteSpace(node.Id))
                throw new InvalidOperationException("Every Cluster:Nodes entry needs an Id.");
            if (!Uri.TryCreate(node.BaseUrl, UriKind.Absolute, out _))
                throw new InvalidOperationException($"Node '{node.Id}' has an invalid BaseUrl: '{node.BaseUrl}'.");
        }

        if (VirtualNodesPerNode <= 0)
            throw new InvalidOperationException("Cluster:VirtualNodesPerNode must be greater than zero.");
        if (ReplicationFactor <= 0)
            throw new InvalidOperationException("Cluster:ReplicationFactor must be greater than zero.");
        if (ReadReplicaPercentage is < 0 or > 100)
            throw new InvalidOperationException("Cluster:ReadReplicaPercentage must be between 0 and 100.");
    }

    /// <summary>Replication factor clamped to the number of nodes actually configured.</summary>
    public int EffectiveReplicationFactor => Math.Min(ReplicationFactor, Math.Max(1, Nodes.Count));
}
