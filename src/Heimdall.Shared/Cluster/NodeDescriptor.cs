namespace Heimdall.Shared.Cluster;

/// <summary>
/// A single physical storage node in the Heimdall cluster. Nodes are identified by a stable
/// string id: the id (not the URL) is what gets hashed onto the ring, so a node can move hosts
/// without reshuffling the keyspace.
/// </summary>
public sealed class NodeDescriptor
{
    public required string Id { get; init; }

    /// <summary>Base address of the node's HTTP API, e.g. <c>http://storage-node-1:8080</c>.</summary>
    public required string BaseUrl { get; init; }

    /// <summary>
    /// Relative capacity. A node with weight 2 claims twice as many virtual nodes on the ring as
    /// a node with weight 1, and therefore owns roughly twice the keyspace.
    /// </summary>
    public int Weight { get; init; } = 1;

    public Uri BaseUri => new(BaseUrl.TrimEnd('/') + "/");

    public override string ToString() => $"{Id} ({BaseUrl}, w={Weight})";
}
