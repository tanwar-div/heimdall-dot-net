namespace Heimdall.Shared.Contracts;

/// <summary>Custom headers Heimdall uses to make routing decisions observable from the outside.</summary>
public static class HeimdallHeaders
{
    /// <summary>Shared secret on write paths.</summary>
    public const string ApiKey = "X-Heimdall-Key";

    /// <summary>Ring primary for the requested video id.</summary>
    public const string PrimaryNode = "X-Heimdall-Primary-Node";

    /// <summary>Node that actually served the bytes. Differs from the primary when reads were load balanced.</summary>
    public const string ServedByNode = "X-Heimdall-Served-By";

    /// <summary>Node ids the gateway tried and gave up on, in order.</summary>
    public const string FailedOverFrom = "X-Heimdall-Failover-From";

    /// <summary>Ordered read candidates the load balancer produced for this request.</summary>
    public const string ReadCandidates = "X-Heimdall-Read-Candidates";

    /// <summary>Set on an inbound write to mark it as replication traffic rather than a client upload.</summary>
    public const string ReplicaWrite = "X-Heimdall-Replica-Write";

    /// <summary>Ring primary asserted by a replicating node, so the replica can record true ownership.</summary>
    public const string OwnerNode = "X-Heimdall-Owner-Node";

    /// <summary>Original file name, propagated through the write path.</summary>
    public const string FileName = "X-Heimdall-File-Name";
}
