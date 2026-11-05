using Heimdall.Shared.Cluster;

namespace Heimdall.Tests;

/// <summary>Shared fixtures so each test file does not hand-roll a six-node cluster.</summary>
internal static class TestCluster
{
    public static List<NodeDescriptor> Nodes(int count, int weight = 1) =>
        Enumerable.Range(1, count)
            .Select(i => new NodeDescriptor
            {
                Id = $"node-{i}",
                BaseUrl = $"http://node-{i}:8080",
                Weight = weight,
            })
            .ToList();

    public static ClusterOptions Options(
        int nodeCount = 6,
        int replicationFactor = 3,
        int readReplicaPercentage = 60,
        int virtualNodes = ConsistentHashRing.DefaultVirtualNodesPerNode) => new()
    {
        Nodes = Nodes(nodeCount),
        ReplicationFactor = replicationFactor,
        ReadReplicaPercentage = readReplicaPercentage,
        VirtualNodesPerNode = virtualNodes,
        ApiKey = "test-key",
    };

    public static ClusterTopology Topology(
        int nodeCount = 6,
        int replicationFactor = 3,
        int readReplicaPercentage = 60) =>
        new(Options(nodeCount, replicationFactor, readReplicaPercentage));
}
