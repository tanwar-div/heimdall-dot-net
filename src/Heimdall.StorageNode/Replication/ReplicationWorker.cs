using Heimdall.Shared.Cluster;
using Heimdall.Shared.Contracts;
using Heimdall.Shared.Http;
using Heimdall.StorageNode.Storage;
using Microsoft.Extensions.Options;

namespace Heimdall.StorageNode.Replication;

/// <summary>
/// Drains <see cref="ReplicationQueue"/> and pushes each newly ingested video to the replicas the
/// ring assigns it.
///
/// The target list is derived locally from the same consistent-hash ring the gateway uses, so no
/// node is told where to replicate - it works it out from the video id. Each target gets its own
/// retry loop with exponential backoff and jitter, and each attempt opens a *fresh* chunk stream,
/// because an HTTP request body cannot be replayed once it has been partly consumed.
/// </summary>
public sealed class ReplicationWorker : BackgroundService
{
    private readonly ReplicationQueue _queue;
    private readonly ChunkedVideoRepository _repository;
    private readonly StorageNodeClient _client;
    private readonly ClusterTopology _topology;
    private readonly NodeOptions _node;
    private readonly ILogger<ReplicationWorker> _logger;
    private readonly RetryPolicy _retry;

    public ReplicationWorker(
        ReplicationQueue queue,
        ChunkedVideoRepository repository,
        StorageNodeClient client,
        ClusterTopology topology,
        IOptions<NodeOptions> node,
        ILogger<ReplicationWorker> logger)
    {
        _queue = queue;
        _repository = repository;
        _client = client;
        _topology = topology;
        _node = node.Value;
        _logger = logger;
        _retry = new RetryPolicy(_node.ReplicationMaxAttempts, _node.ReplicationBaseDelay, _node.ReplicationMaxDelay);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_node.ReplicationEnabled)
        {
            _logger.LogWarning("Replication is disabled on {NodeId}; uploads will be stored on the primary only.", _node.Id);
            return;
        }

        _logger.LogInformation(
            "Replication worker started on {NodeId} (replication factor {Factor}, up to {Attempts} attempts per target).",
            _node.Id, _topology.ReplicationFactor, _retry.MaxAttempts);

        await foreach (var job in _queue.ReadAllAsync(stoppingToken).ConfigureAwait(false))
        {
            try
            {
                await ReplicateAsync(job, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Replication of {VideoId} failed unexpectedly.", job.VideoId);
            }
        }
    }

    private async Task ReplicateAsync(ReplicationJob job, CancellationToken cancellationToken)
    {
        var targets = _topology.GetReplicationTargets(job.VideoId, _node.Id);
        if (targets.Count == 0)
        {
            _logger.LogDebug("{NodeId} is not the primary for {VideoId}; nothing to replicate.", _node.Id, job.VideoId);
            return;
        }

        var manifest = await _repository.GetManifestAsync(job.VideoId, cancellationToken).ConfigureAwait(false);
        if (manifest is null)
        {
            _logger.LogWarning("Cannot replicate {VideoId}: no local manifest.", job.VideoId);
            return;
        }

        // Targets are pushed concurrently: replica latency is additive otherwise, and the copies
        // are independent of each other.
        var pushes = targets.Select(target => PushToTargetAsync(manifest, target, cancellationToken));
        await Task.WhenAll(pushes).ConfigureAwait(false);
    }

    private async Task PushToTargetAsync(VideoManifest manifest, NodeDescriptor target, CancellationToken cancellationToken)
    {
        Exception? lastError = null;
        string? lastFailureReason = null;
        var attemptsUsed = 0;

        _queue.Report(new ReplicationStatus
        {
            VideoId = manifest.VideoId,
            TargetNodeId = target.Id,
            State = "pending",
        });

        var succeeded = await _retry.ExecuteAsync(
            async (attempt, token) =>
            {
                attemptsUsed = attempt;

                // Bound the attempt. A replica that vanished mid-transfer never closes its socket,
                // and an unbounded attempt would park this worker on it forever.
                using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(token);
                attemptCts.CancelAfter(_node.ReplicationAttemptTimeout);
                token = attemptCts.Token;

                // Already there (an earlier attempt landed, or the replica was restored out of band)?
                var existing = await _client.GetManifestAsync(target, manifest.VideoId, token).ConfigureAwait(false);
                if (existing is not null && existing.ContentHash == manifest.ContentHash)
                {
                    _logger.LogDebug(
                        "{VideoId} already present on {TargetId} with matching hash; skipping copy.",
                        manifest.VideoId, target.Id);
                    return true;
                }

                // A fresh stream per attempt: a partly-consumed body cannot be resent.
                await using var body = _repository.OpenFullStream(manifest);

                using var response = await _client.PutVideoAsync(
                    target,
                    manifest.VideoId,
                    body,
                    manifest.ContentType,
                    manifest.FileName,
                    contentLength: manifest.Length,
                    isReplicaWrite: true,
                    ownerNodeId: manifest.OwnerNodeId,
                    cancellationToken: token).ConfigureAwait(false);

                if (response.IsSuccessStatusCode) return true;

                lastFailureReason = $"HTTP {(int)response.StatusCode} {response.ReasonPhrase}";
                return false;
            },
            onFailure: (attempt, ex) =>
            {
                lastError = ex ?? lastError;
                var reason = ex?.Message ?? lastFailureReason ?? "unknown";
                _logger.LogWarning(
                    "Replication of {VideoId} to {TargetId} failed on attempt {Attempt}/{MaxAttempts}: {Reason}",
                    manifest.VideoId, target.Id, attempt, _retry.MaxAttempts, reason);

                _queue.Report(new ReplicationStatus
                {
                    VideoId = manifest.VideoId,
                    TargetNodeId = target.Id,
                    State = attempt >= _retry.MaxAttempts ? "failed" : "retrying",
                    Attempts = attempt,
                    Error = reason,
                });
            },
            logger: _logger,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        _queue.Report(new ReplicationStatus
        {
            VideoId = manifest.VideoId,
            TargetNodeId = target.Id,
            State = succeeded ? "replicated" : "failed",
            Attempts = attemptsUsed,
            Error = succeeded ? null : (lastError?.Message ?? lastFailureReason),
        });

        if (succeeded)
        {
            _logger.LogInformation(
                "Replicated {VideoId} ({Length} bytes) from {NodeId} to {TargetId} in {Attempts} attempt(s).",
                manifest.VideoId, manifest.Length, _node.Id, target.Id, attemptsUsed);
        }
        else
        {
            _logger.LogError(
                "Gave up replicating {VideoId} to {TargetId} after {Attempts} attempt(s). The video is still readable from {NodeId}.",
                manifest.VideoId, target.Id, attemptsUsed, _node.Id);
        }
    }
}
