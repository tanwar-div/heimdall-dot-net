using Heimdall.Shared.Cluster;

namespace Heimdall.Tests;

public class ReplicaLoadBalancerTests
{
    private static IReadOnlyList<NodeDescriptor> Preference(int count) => TestCluster.Nodes(count);

    [Theory]
    [InlineData(0, 5, 0)]
    [InlineData(100, 5, 5)]
    [InlineData(60, 5, 3)]
    [InlineData(50, 5, 3)]    // ceil(2.5) - round up so a non-zero percentage always picks someone
    [InlineData(20, 5, 1)]
    [InlineData(1, 5, 1)]     // never silently degrade to zero replicas
    [InlineData(60, 0, 0)]
    [InlineData(60, 1, 1)]
    public void Selection_size_rounds_up_and_clamps(int percentage, int replicaCount, int expected)
        => Assert.Equal(expected, new ReplicaLoadBalancer(percentage).ReplicaSelectionSize(replicaCount));

    [Fact]
    public void Rejects_percentages_outside_zero_to_one_hundred()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ReplicaLoadBalancer(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ReplicaLoadBalancer(101));
    }

    [Fact]
    public void Candidate_list_is_a_permutation_of_the_preference_list()
    {
        // The candidate list doubles as the failover chain, so dropping or duplicating a holder
        // would mean either an unreachable copy or a wasted retry.
        var preference = Preference(5);
        var balancer = new ReplicaLoadBalancer(60);

        for (var rotation = 0; rotation < 20; rotation++)
        {
            var candidates = balancer.SelectReadCandidates(preference, rotation);

            Assert.Equal(preference.Count, candidates.Count);
            Assert.Equal(
                preference.Select(n => n.Id).OrderBy(id => id),
                candidates.Select(n => n.Id).OrderBy(id => id));
        }
    }

    [Fact]
    public void Zero_percent_reads_from_the_primary_first_and_keeps_replicas_as_failover()
    {
        var preference = Preference(4);
        var candidates = new ReplicaLoadBalancer(0).SelectReadCandidates(preference, rotation: 7);

        Assert.Equal("node-1", candidates[0].Id);
        Assert.Equal(preference.Select(n => n.Id), candidates.Select(n => n.Id));
    }

    [Fact]
    public void One_hundred_percent_puts_every_replica_ahead_of_the_primary()
    {
        var preference = Preference(4);
        var candidates = new ReplicaLoadBalancer(100).SelectReadCandidates(preference, rotation: 0);

        // Primary last: it is reserved for writes and for failover.
        Assert.Equal("node-1", candidates[^1].Id);
        Assert.DoesNotContain("node-1", candidates.Take(3).Select(n => n.Id));
    }

    [Fact]
    public void Partial_percentage_puts_the_primary_after_the_selected_replicas_but_before_the_rest()
    {
        var preference = Preference(5);          // primary + 4 replicas
        var candidates = new ReplicaLoadBalancer(50).SelectReadCandidates(preference, rotation: 0);

        // ceil(4 * 0.5) = 2 replicas selected, then the primary, then the 2 unselected replicas.
        Assert.Equal(["node-2", "node-3", "node-1", "node-4", "node-5"], candidates.Select(n => n.Id));
    }

    [Fact]
    public void Rotation_spreads_first_choice_evenly_across_the_selected_replicas()
    {
        // This is the "spread traffic" claim, measured: with 60% of 2 replicas selected, repeated
        // reads of one hot video must not all land on the same node.
        var preference = Preference(3);
        var balancer = new ReplicaLoadBalancer(60);

        var firstChoice = new Dictionary<string, int>();
        for (var rotation = 0; rotation < 600; rotation++)
        {
            var winner = balancer.SelectReadCandidates(preference, rotation)[0].Id;
            firstChoice[winner] = firstChoice.GetValueOrDefault(winner) + 1;
        }

        Assert.Equal(["node-2", "node-3"], firstChoice.Keys.OrderBy(k => k));
        Assert.Equal(300, firstChoice["node-2"]);
        Assert.Equal(300, firstChoice["node-3"]);
    }

    [Fact]
    public void Stateful_overload_also_rotates_rather_than_pinning_one_replica()
    {
        var preference = Preference(4);
        var balancer = new ReplicaLoadBalancer(100);

        var seen = new HashSet<string>();
        for (var i = 0; i < 30; i++)
            seen.Add(balancer.SelectReadCandidates(preference)[0].Id);

        Assert.Equal(3, seen.Count);               // all three replicas take a turn at the front
        Assert.DoesNotContain("node-1", seen);
    }

    [Fact]
    public void Negative_rotation_counters_do_not_throw()
    {
        // The internal counter is a wrapping Interlocked.Increment, so it eventually goes negative.
        var candidates = new ReplicaLoadBalancer(60)
            .SelectReadCandidates(Preference(4), rotation: int.MinValue);

        Assert.Equal(4, candidates.Count);
    }

    [Fact]
    public void Single_node_cluster_returns_the_preference_list_untouched()
    {
        var preference = Preference(1);
        Assert.Same(preference, new ReplicaLoadBalancer(100).SelectReadCandidates(preference, 0));
    }

    [Fact]
    public void Topology_reports_replication_targets_only_for_the_primary()
    {
        var topology = TestCluster.Topology(nodeCount: 6, replicationFactor: 3);
        var placement = topology.GetPlacement("video-42");

        var fromPrimary = topology.GetReplicationTargets("video-42", placement.Primary.Id);
        Assert.Equal(placement.Replicas.Select(n => n.Id), fromPrimary.Select(n => n.Id));

        // A replica asked the same question answers "not mine", which is how a node avoids
        // re-replicating traffic it received from the primary.
        Assert.Empty(topology.GetReplicationTargets("video-42", placement.Replicas[0].Id));
        Assert.Empty(topology.GetReplicationTargets("video-42", "node-does-not-exist"));
    }

    [Fact]
    public void Replication_factor_is_clamped_to_the_cluster_size()
    {
        var topology = TestCluster.Topology(nodeCount: 2, replicationFactor: 5);

        Assert.Equal(2, topology.ReplicationFactor);
        Assert.Single(topology.GetPlacement("video-1").Replicas);
    }
}
