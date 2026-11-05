using System.Collections.Concurrent;
using System.Threading.Channels;
using Heimdall.Shared.Contracts;

namespace Heimdall.StorageNode.Replication;

public sealed record ReplicationJob(string VideoId, DateTimeOffset EnqueuedAt)
{
    public static ReplicationJob For(string videoId) => new(videoId, DateTimeOffset.UtcNow);
}

/// <summary>
/// Hand-off point between the ingest request and the background replicator.
///
/// Replication is asynchronous on purpose: the upload response returns as soon as the primary has
/// durably stored the video, and fan-out to replicas proceeds behind it. A client therefore pays
/// one write, not <c>ReplicationFactor</c> writes, and a slow or dead replica cannot stall an
/// upload. The trade-off is a window in which only the primary holds the video, during which a
/// read that load-balances onto a replica falls over to the primary.
/// </summary>
public sealed class ReplicationQueue
{
    private readonly Channel<ReplicationJob> _channel =
        Channel.CreateUnbounded<ReplicationJob>(new UnboundedChannelOptions { SingleReader = true });

    private readonly ConcurrentDictionary<string, ReplicationStatus> _status = new(StringComparer.Ordinal);

    public void Enqueue(ReplicationJob job) => _channel.Writer.TryWrite(job);

    public IAsyncEnumerable<ReplicationJob> ReadAllAsync(CancellationToken cancellationToken)
        => _channel.Reader.ReadAllAsync(cancellationToken);

    public void Complete() => _channel.Writer.TryComplete();

    public void Report(ReplicationStatus status)
        => _status[$"{status.VideoId}|{status.TargetNodeId}"] = status;

    /// <summary>Every tracked primary-to-replica copy on this node, newest first. Diagnostics only.</summary>
    public IReadOnlyList<ReplicationStatus> Snapshot()
        => _status.Values.OrderByDescending(static s => s.UpdatedAt).ToArray();

    public IReadOnlyList<ReplicationStatus> SnapshotFor(string videoId)
        => _status.Values
            .Where(s => string.Equals(s.VideoId, videoId, StringComparison.Ordinal))
            .OrderBy(static s => s.TargetNodeId, StringComparer.Ordinal)
            .ToArray();
}
