using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Heimdall.Shared.Cluster;

namespace Heimdall.Tests;

public class ConsistentHashRingTests
{
    [Fact]
    public void HashKey_is_the_first_eight_bytes_of_sha256_big_endian()
    {
        // Routing only works if every process in the cluster derives the same token for a key, so
        // the hash is pinned to a documented, platform-independent construction.
        const string key = "interstellar-4k.mp4";

        var digest = SHA256.HashData(Encoding.UTF8.GetBytes(key));
        var expected = BinaryPrimitives.ReadUInt64BigEndian(digest);

        Assert.Equal(expected, ConsistentHashRing.HashKey(key));
    }

    [Fact]
    public void Placement_is_identical_across_independently_constructed_rings()
    {
        // This is the property that lets the gateway and the storage nodes route without talking to
        // each other: two separately built rings over the same node set must agree exactly.
        var a = new ConsistentHashRing(TestCluster.Nodes(6));
        var b = new ConsistentHashRing(TestCluster.Nodes(6).OrderByDescending(n => n.Id).ToList());

        for (var i = 0; i < 2_000; i++)
        {
            var key = $"video-{i}";
            Assert.Equal(a.GetPrimary(key).Id, b.GetPrimary(key).Id);
            Assert.Equal(
                a.GetPreferenceList(key, 3).Select(n => n.Id),
                b.GetPreferenceList(key, 3).Select(n => n.Id));
        }
    }

    [Fact]
    public void Preference_list_starts_with_the_primary_and_holds_distinct_nodes()
    {
        var ring = new ConsistentHashRing(TestCluster.Nodes(6));

        for (var i = 0; i < 500; i++)
        {
            var key = $"video-{i}";
            var preference = ring.GetPreferenceList(key, 3);

            Assert.Equal(3, preference.Count);
            Assert.Equal(ring.GetPrimary(key).Id, preference[0].Id);
            Assert.Equal(3, preference.Select(n => n.Id).Distinct().Count());
        }
    }

    [Fact]
    public void Preference_list_is_capped_at_the_cluster_size()
    {
        var ring = new ConsistentHashRing(TestCluster.Nodes(2));
        Assert.Equal(2, ring.GetPreferenceList("video-1", 5).Count);
    }

    [Fact]
    public void The_default_virtual_node_count_keeps_the_keyspace_within_ten_percent_of_even()
    {
        const int keys = 120_000;
        var worst = WorstDeviation(ConsistentHashRing.DefaultVirtualNodesPerNode, keys);

        Assert.True(worst < 0.10, $"Worst-case deviation was {worst:P2} at the default token count.");
    }

    [Fact]
    public void More_virtual_nodes_flatten_the_distribution()
    {
        // The reason the default is what it is. A single token per node leaves one node owning
        // nearly twice its share; the imbalance shrinks roughly as 1/sqrt(tokens).
        var single = WorstDeviation(1, 60_000);
        var modest = WorstDeviation(64, 60_000);
        var standard = WorstDeviation(ConsistentHashRing.DefaultVirtualNodesPerNode, 60_000);

        Assert.True(single > 0.5, $"A one-token ring should be badly skewed, but was only {single:P2} off.");
        Assert.True(modest < single, $"64 tokens ({modest:P2}) should beat 1 token ({single:P2}).");
        Assert.True(standard < modest, $"The default ({standard:P2}) should beat 64 tokens ({modest:P2}).");
    }

    private static double WorstDeviation(int virtualNodes, int keys)
    {
        var ring = new ConsistentHashRing(TestCluster.Nodes(6), virtualNodes);
        var counts = ring.Nodes.ToDictionary(n => n.Id, _ => 0);

        for (var i = 0; i < keys; i++)
            counts[ring.GetPrimary($"video-{i}").Id]++;

        Assert.Equal(keys, counts.Values.Sum());

        var ideal = keys / 6.0;
        return counts.Values.Max(c => Math.Abs(c - ideal)) / ideal;
    }

    [Fact]
    public void Weight_scales_a_nodes_share_of_the_keyspace()
    {
        var nodes = TestCluster.Nodes(3);
        nodes[0] = new NodeDescriptor { Id = "node-1", BaseUrl = "http://node-1:8080", Weight = 3 };

        var ring = new ConsistentHashRing(nodes, virtualNodesPerNode: 300);

        const int keys = 90_000;
        var counts = ring.Nodes.ToDictionary(n => n.Id, _ => 0);
        for (var i = 0; i < keys; i++)
            counts[ring.GetPrimary($"video-{i}").Id]++;

        // node-1 holds 3 of the 5 weight units, so it should own roughly 60% of the keyspace.
        var share = counts["node-1"] / (double)keys;
        Assert.InRange(share, 0.55, 0.65);
    }

    [Fact]
    public void Ownership_shares_sum_to_the_whole_keyspace()
    {
        var ring = new ConsistentHashRing(TestCluster.Nodes(6));
        var shares = ring.OwnershipShares();

        Assert.Equal(6, shares.Count);
        Assert.Equal(1.0, shares.Values.Sum(), precision: 6);
        foreach (var share in shares.Values)
            Assert.InRange(share, 0.10, 0.24);
    }

    [Fact]
    public void Adding_a_node_moves_only_its_own_share_of_keys_and_moves_them_only_to_itself()
    {
        // The reason to use consistent hashing instead of modulo: scaling out must not reshuffle
        // the whole keyspace, because every moved key is a video that has to be copied.
        const int keys = 60_000;
        var before = new ConsistentHashRing(TestCluster.Nodes(6));
        var after = new ConsistentHashRing(TestCluster.Nodes(7));

        var moved = 0;
        for (var i = 0; i < keys; i++)
        {
            var key = $"video-{i}";
            var old = before.GetPrimary(key).Id;
            var now = after.GetPrimary(key).Id;

            if (old == now) continue;

            moved++;
            // Any key that moved must have moved onto the newly added node, never between two
            // pre-existing nodes.
            Assert.Equal("node-7", now);
        }

        var fraction = moved / (double)keys;
        // Ideal is 1/7 ~= 14.3%; allow ring noise but reject anything resembling a full reshuffle.
        Assert.InRange(fraction, 0.10, 0.19);
    }

    [Fact]
    public void Removing_a_node_only_remaps_the_keys_it_owned()
    {
        const int keys = 60_000;
        var before = new ConsistentHashRing(TestCluster.Nodes(6));
        var after = new ConsistentHashRing(TestCluster.Nodes(5));

        var moved = 0;
        for (var i = 0; i < keys; i++)
        {
            var key = $"video-{i}";
            var old = before.GetPrimary(key).Id;
            var now = after.GetPrimary(key).Id;

            if (old == now) continue;

            moved++;
            Assert.Equal("node-6", old);   // only keys owned by the departed node may move
        }

        Assert.InRange(moved / (double)keys, 0.12, 0.22);
    }

    [Fact]
    public void A_single_node_cluster_owns_every_key_and_has_a_one_entry_preference_list()
    {
        var ring = new ConsistentHashRing(TestCluster.Nodes(1));

        Assert.Equal(1, ring.NodeCount);
        Assert.Equal("node-1", ring.GetPrimary("anything").Id);
        Assert.Single(ring.GetPreferenceList("anything", 3));
    }

    [Fact]
    public void Ring_rejects_invalid_configurations()
    {
        Assert.Throws<ArgumentException>(() => new ConsistentHashRing([]));

        Assert.Throws<ArgumentException>(() => new ConsistentHashRing(
        [
            new NodeDescriptor { Id = "node-1", BaseUrl = "http://a:1" },
            new NodeDescriptor { Id = "node-1", BaseUrl = "http://b:1" },
        ]));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ConsistentHashRing(TestCluster.Nodes(2), virtualNodesPerNode: 0));
    }

    [Fact]
    public void Token_count_reflects_virtual_nodes_and_weights()
    {
        var nodes = TestCluster.Nodes(3);
        nodes[0] = new NodeDescriptor { Id = "node-1", BaseUrl = "http://node-1:8080", Weight = 2 };

        var ring = new ConsistentHashRing(nodes, virtualNodesPerNode: 10);

        Assert.Equal(40, ring.TokenCount);           // 2*10 + 1*10 + 1*10
        Assert.Equal(40, ring.Snapshot().Count);
    }

    [Fact]
    public void Snapshot_is_sorted_by_token()
    {
        var tokens = new ConsistentHashRing(TestCluster.Nodes(4), virtualNodesPerNode: 20)
            .Snapshot()
            .Select(t => t.Token)
            .ToArray();

        Assert.Equal(tokens.OrderBy(t => t).ToArray(), tokens);
    }

    [Fact]
    public void Keys_hashing_past_the_last_token_wrap_to_the_first()
    {
        // The seam is the one place an off-by-one in the binary search would hide, so it gets its
        // own assertion: ulong.MaxValue is past every token with overwhelming probability.
        var ring = new ConsistentHashRing(TestCluster.Nodes(4), virtualNodesPerNode: 4);
        var snapshot = ring.Snapshot();

        Assert.Equal(snapshot[0].NodeId, ring.GetPrimary(FindKeyHashingAbove(snapshot[^1].Token)).Id);
    }

    private static string FindKeyHashingAbove(ulong token)
    {
        for (var i = 0; i < 1_000_000; i++)
        {
            var key = $"seam-probe-{i}";
            if (ConsistentHashRing.HashKey(key) > token) return key;
        }

        throw new InvalidOperationException("No probe key hashed above the final token.");
    }
}
