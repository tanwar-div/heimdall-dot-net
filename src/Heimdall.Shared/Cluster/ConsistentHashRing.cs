using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace Heimdall.Shared.Cluster;

/// <summary>One virtual node (token) placed on the ring.</summary>
public readonly record struct RingToken(ulong Token, string NodeId, int Replica);

/// <summary>
/// A consistent-hashing ring with virtual nodes.
///
/// Every physical node is projected onto the ring <c>VirtualNodesPerNode * Weight</c> times. A key
/// is hashed into the same 64-bit space and walks clockwise to the first token; that token's node
/// is the key's primary. Continuing clockwise past tokens belonging to already-seen nodes yields
/// the preference list (primary + replicas).
///
/// Two properties matter for Heimdall:
/// <list type="bullet">
///   <item>Placement is a pure function of the key and the node set, so gateway and storage nodes
///   independently compute identical placements with no central lookup table.</item>
///   <item>Adding or removing a node only moves the keys that fall in the arcs it claims or
///   releases, i.e. roughly <c>1/N</c> of the keyspace instead of all of it.</item>
/// </list>
///
/// The hash is SHA-256 truncated to its first 8 bytes (big-endian). It must be stable across
/// processes, platforms and runs, which rules out <see cref="string.GetHashCode()"/>.
/// </summary>
public sealed class ConsistentHashRing
{
    private readonly ulong[] _tokens;          // sorted ascending
    private readonly string[] _owners;         // _owners[i] owns _tokens[i]
    private readonly Dictionary<string, NodeDescriptor> _nodes;

    /// <summary>
    /// Measured on a six-node ring over 120k keys: 1 token/node leaves the worst node 89% off an
    /// even share, 160 tokens 17%, 512 tokens 5.5%, and 1024 tokens only 2.7% better than 512.
    /// 512 is where the curve flattens, and 3,072 tokens for six nodes costs nothing to build or
    /// binary-search.
    /// </summary>
    public const int DefaultVirtualNodesPerNode = 512;

    public ConsistentHashRing(IEnumerable<NodeDescriptor> nodes, int virtualNodesPerNode = DefaultVirtualNodesPerNode)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        if (virtualNodesPerNode <= 0)
            throw new ArgumentOutOfRangeException(nameof(virtualNodesPerNode), "A ring needs at least one virtual node per physical node.");

        VirtualNodesPerNode = virtualNodesPerNode;
        _nodes = new Dictionary<string, NodeDescriptor>(StringComparer.Ordinal);

        var placed = new List<RingToken>();
        foreach (var node in nodes)
        {
            if (string.IsNullOrWhiteSpace(node.Id))
                throw new ArgumentException("Node id must be a non-empty string.", nameof(nodes));
            if (!_nodes.TryAdd(node.Id, node))
                throw new ArgumentException($"Duplicate node id '{node.Id}' in cluster configuration.", nameof(nodes));

            var weight = Math.Max(1, node.Weight);
            var vnodes = virtualNodesPerNode * weight;
            for (var i = 0; i < vnodes; i++)
                placed.Add(new RingToken(HashKey($"{node.Id}#{i}"), node.Id, i));
        }

        if (_nodes.Count == 0)
            throw new ArgumentException("A consistent hash ring needs at least one node.", nameof(nodes));

        // Collisions in a 64-bit space are vanishingly unlikely, but ordering by (token, nodeId)
        // keeps the ring deterministic even if two tokens land on the same point.
        placed.Sort(static (a, b) =>
        {
            var byToken = a.Token.CompareTo(b.Token);
            return byToken != 0 ? byToken : string.CompareOrdinal(a.NodeId, b.NodeId);
        });

        _tokens = new ulong[placed.Count];
        _owners = new string[placed.Count];
        for (var i = 0; i < placed.Count; i++)
        {
            _tokens[i] = placed[i].Token;
            _owners[i] = placed[i].NodeId;
        }
    }

    public int VirtualNodesPerNode { get; }

    /// <summary>Number of physical nodes on the ring.</summary>
    public int NodeCount => _nodes.Count;

    /// <summary>Number of virtual nodes (tokens) on the ring.</summary>
    public int TokenCount => _tokens.Length;

    public IReadOnlyCollection<NodeDescriptor> Nodes => _nodes.Values;

    public bool TryGetNode(string nodeId, out NodeDescriptor? node)
    {
        var found = _nodes.TryGetValue(nodeId, out var value);
        node = value;
        return found;
    }

    /// <summary>The node that owns <paramref name="key"/>: all writes for that key land here first.</summary>
    public NodeDescriptor GetPrimary(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        return _nodes[_owners[IndexFor(HashKey(key))]];
    }

    /// <summary>
    /// The ordered preference list for <paramref name="key"/>: index 0 is the primary, then up to
    /// <paramref name="count"/> - 1 distinct replicas found by continuing clockwise. Returns fewer
    /// entries than requested when the cluster has fewer nodes than that.
    /// </summary>
    public IReadOnlyList<NodeDescriptor> GetPreferenceList(string key, int count)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        if (count <= 0) return Array.Empty<NodeDescriptor>();

        var wanted = Math.Min(count, _nodes.Count);
        var result = new List<NodeDescriptor>(wanted);
        var seen = new HashSet<string>(StringComparer.Ordinal);

        var start = IndexFor(HashKey(key));
        for (var step = 0; step < _tokens.Length && result.Count < wanted; step++)
        {
            var owner = _owners[(start + step) % _tokens.Length];
            if (seen.Add(owner))
                result.Add(_nodes[owner]);
        }

        return result;
    }

    /// <summary>Ring contents, ascending by token. Exposed for the gateway's introspection endpoint.</summary>
    public IReadOnlyList<RingToken> Snapshot()
    {
        var snapshot = new RingToken[_tokens.Length];
        for (var i = 0; i < _tokens.Length; i++)
            snapshot[i] = new RingToken(_tokens[i], _owners[i], i);
        return snapshot;
    }

    /// <summary>
    /// Fraction of the 64-bit keyspace owned by each node, derived from the gaps between tokens.
    /// With enough virtual nodes these converge on the nodes' relative weights.
    /// </summary>
    public IReadOnlyDictionary<string, double> OwnershipShares()
    {
        var shares = _nodes.Keys.ToDictionary(id => id, _ => 0d, StringComparer.Ordinal);
        if (_tokens.Length == 0) return shares;

        for (var i = 0; i < _tokens.Length; i++)
        {
            // The arc that ends at token i is owned by token i (clockwise walk from the previous token).
            var previous = i == 0 ? _tokens[^1] : _tokens[i - 1];
            var arc = _tokens[i] - previous;       // unchecked wrap-around is exactly what we want at the seam
            shares[_owners[i]] += arc / (double)ulong.MaxValue;
        }

        return shares;
    }

    /// <summary>Index of the first token at or clockwise-after <paramref name="hash"/>, wrapping at the seam.</summary>
    private int IndexFor(ulong hash)
    {
        var lo = 0;
        var hi = _tokens.Length - 1;
        var found = -1;

        while (lo <= hi)
        {
            var mid = lo + ((hi - lo) >> 1);
            if (_tokens[mid] >= hash)
            {
                found = mid;
                hi = mid - 1;
            }
            else
            {
                lo = mid + 1;
            }
        }

        return found >= 0 ? found : 0;   // past the last token: wrap around to the first
    }

    /// <summary>Stable 64-bit hash: first 8 bytes of SHA-256, big-endian.</summary>
    public static ulong HashKey(string value)
    {
        Span<byte> digest = stackalloc byte[32];
        var maxBytes = Encoding.UTF8.GetMaxByteCount(value.Length);
        byte[]? rented = null;
        try
        {
            Span<byte> input = maxBytes <= 256
                ? stackalloc byte[256]
                : (rented = System.Buffers.ArrayPool<byte>.Shared.Rent(maxBytes));

            var written = Encoding.UTF8.GetBytes(value, input);
            SHA256.HashData(input[..written], digest);
            return BinaryPrimitives.ReadUInt64BigEndian(digest);
        }
        finally
        {
            if (rented is not null) System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        }
    }
}
