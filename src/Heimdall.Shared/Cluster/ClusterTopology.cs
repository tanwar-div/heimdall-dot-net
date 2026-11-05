using Microsoft.Extensions.Options;

namespace Heimdall.Shared.Cluster;

/// <summary>Where a video lives and in what order to ask for it.</summary>
public sealed record VideoPlacement(
    string VideoId,
    NodeDescriptor Primary,
    IReadOnlyList<NodeDescriptor> Replicas,
    IReadOnlyList<NodeDescriptor> ReadCandidates)
{
    /// <summary>Primary first, then replicas: every node that should hold this video.</summary>
    public IReadOnlyList<NodeDescriptor> Holders => [Primary, .. Replicas];
}

/// <summary>
/// The cluster view shared by the gateway and every storage node: a consistent-hash ring plus the
/// read load balancer layered on top of it. Registered as a singleton; placement is computed on
/// demand from the key, so no process keeps a video-to-node table.
/// </summary>
public sealed class ClusterTopology
{
    private readonly ClusterOptions _options;

    public ClusterTopology(IOptions<ClusterOptions> options)
        : this(options.Value) { }

    public ClusterTopology(ClusterOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        _options = options;
        Ring = new ConsistentHashRing(options.Nodes, options.VirtualNodesPerNode);
        LoadBalancer = new ReplicaLoadBalancer(options.ReadReplicaPercentage);
    }

    public ConsistentHashRing Ring { get; }

    public ReplicaLoadBalancer LoadBalancer { get; }

    public ClusterOptions Options => _options;

    public int ReplicationFactor => _options.EffectiveReplicationFactor;

    /// <summary>Full placement decision for a video id, including the ordered read/failover chain.</summary>
    public VideoPlacement GetPlacement(string videoId)
    {
        var preferenceList = Ring.GetPreferenceList(videoId, ReplicationFactor);
        var candidates = LoadBalancer.SelectReadCandidates(preferenceList);
        return new VideoPlacement(
            videoId,
            preferenceList[0],
            preferenceList.Skip(1).ToArray(),
            candidates);
    }

    /// <summary>Owning node for a video id. Writes always go here first.</summary>
    public NodeDescriptor GetPrimary(string videoId) => Ring.GetPrimary(videoId);

    /// <summary>
    /// Replicas that <paramref name="nodeId"/> must push a video to, given that it is the primary.
    /// Returns an empty list when the node is not the primary for that key, which is how a storage
    /// node decides locally whether it owes anyone a copy.
    /// </summary>
    public IReadOnlyList<NodeDescriptor> GetReplicationTargets(string videoId, string nodeId)
    {
        var preferenceList = Ring.GetPreferenceList(videoId, ReplicationFactor);
        if (preferenceList.Count == 0 || !string.Equals(preferenceList[0].Id, nodeId, StringComparison.Ordinal))
            return Array.Empty<NodeDescriptor>();

        return preferenceList.Skip(1).ToArray();
    }
}
