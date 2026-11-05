using Heimdall.Shared.Streaming;

namespace Heimdall.StorageNode;

/// <summary>Identity and tuning for this storage-node process, bound from the <c>Node</c> section.</summary>
public sealed class NodeOptions
{
    public const string SectionName = "Node";

    /// <summary>
    /// This node's id. It must match one of the ids in <c>Cluster:Nodes</c>, because that is the
    /// string the ring hashes; a mismatch means the node can never be anyone's primary.
    /// </summary>
    public string Id { get; set; } = "node-1";

    /// <summary>Bytes per stored chunk. Larger chunks mean fewer object-storage round trips per seek.</summary>
    public long ChunkSizeBytes { get; set; } = ChunkMap.DefaultChunkSize;

    /// <summary>Replicate newly ingested videos to the ring's other holders.</summary>
    public bool ReplicationEnabled { get; set; } = true;

    /// <summary>Attempts per replication target before the copy is abandoned.</summary>
    public int ReplicationMaxAttempts { get; set; } = 4;

    public TimeSpan ReplicationBaseDelay { get; set; } = TimeSpan.FromSeconds(2);

    public TimeSpan ReplicationMaxDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Ceiling on a single replication attempt. A replica that is killed or partitioned sends no
    /// RST, so without this the worker would block on the dead peer indefinitely instead of backing
    /// off and retrying. Generous by default, because the attempt is a whole-video transfer.
    /// </summary>
    public TimeSpan ReplicationAttemptTimeout { get; set; } = TimeSpan.FromMinutes(10);

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(Id))
            throw new InvalidOperationException("Node:Id must be set.");
        if (ChunkSizeBytes < 64 * 1024)
            throw new InvalidOperationException("Node:ChunkSizeBytes must be at least 64 KiB.");
        if (ReplicationMaxAttempts < 1)
            throw new InvalidOperationException("Node:ReplicationMaxAttempts must be at least 1.");
        if (ReplicationAttemptTimeout <= TimeSpan.Zero)
            throw new InvalidOperationException("Node:ReplicationAttemptTimeout must be positive.");
    }
}

/// <summary>Object-storage connection settings, bound from the <c>Storage</c> section.</summary>
public sealed class StorageOptions
{
    public const string SectionName = "Storage";

    /// <summary>S3-compatible endpoint, e.g. the MinIO service address <c>http://minio:9000</c>.</summary>
    public string Endpoint { get; set; } = "http://localhost:9000";

    public string AccessKey { get; set; } = "minioadmin";

    public string SecretKey { get; set; } = "minioadmin";

    /// <summary>
    /// Bucket this node owns. Each node gets its own bucket, so a replica is a genuinely separate
    /// copy of the bytes rather than a second pointer at the same object.
    /// </summary>
    public string Bucket { get; set; } = "heimdall-node-1";

    public string Region { get; set; } = "us-east-1";

    /// <summary>MinIO needs path-style addressing; virtual-host style assumes DNS per bucket.</summary>
    public bool ForcePathStyle { get; set; } = true;

    /// <summary>Create the bucket on startup if it does not exist.</summary>
    public bool CreateBucketIfMissing { get; set; } = true;

    public void Validate()
    {
        if (!Uri.TryCreate(Endpoint, UriKind.Absolute, out _))
            throw new InvalidOperationException($"Storage:Endpoint is not a valid absolute URI: '{Endpoint}'.");
        if (string.IsNullOrWhiteSpace(Bucket))
            throw new InvalidOperationException("Storage:Bucket must be set.");
    }
}
