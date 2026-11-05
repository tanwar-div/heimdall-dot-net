using Microsoft.Extensions.Logging;

namespace Heimdall.Shared.Http;

/// <summary>Exponential backoff with full jitter, used on the primary-to-replica replication path.</summary>
public sealed class RetryPolicy
{
    private readonly int _maxAttempts;
    private readonly TimeSpan _baseDelay;
    private readonly TimeSpan _maxDelay;

    public RetryPolicy(int maxAttempts = 4, TimeSpan? baseDelay = null, TimeSpan? maxDelay = null)
    {
        if (maxAttempts < 1) throw new ArgumentOutOfRangeException(nameof(maxAttempts));
        _maxAttempts = maxAttempts;
        _baseDelay = baseDelay ?? TimeSpan.FromSeconds(1);
        _maxDelay = maxDelay ?? TimeSpan.FromSeconds(30);
    }

    public int MaxAttempts => _maxAttempts;

    /// <summary>
    /// Delay before attempt number <paramref name="attempt"/> (1-based). Full jitter keeps a group
    /// of replicas from retrying a recovering node in lockstep.
    /// </summary>
    public TimeSpan DelayFor(int attempt)
    {
        if (attempt <= 1) return TimeSpan.Zero;

        var exponent = Math.Min(attempt - 1, 16);               // cap before the shift overflows
        var scaled = _baseDelay.TotalMilliseconds * Math.Pow(2, exponent - 1);
        var capped = Math.Min(scaled, _maxDelay.TotalMilliseconds);
        var jittered = Random.Shared.NextDouble() * capped;
        return TimeSpan.FromMilliseconds(jittered);
    }

    /// <summary>
    /// Runs <paramref name="operation"/> until it reports success or attempts run out. The operation
    /// receives the 1-based attempt number; it returns true on success. Exceptions are treated as
    /// retryable failures and surfaced through <paramref name="onFailure"/>.
    /// </summary>
    public async Task<bool> ExecuteAsync(
        Func<int, CancellationToken, Task<bool>> operation,
        Action<int, Exception?>? onFailure = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        for (var attempt = 1; attempt <= _maxAttempts; attempt++)
        {
            var delay = DelayFor(attempt);
            if (delay > TimeSpan.Zero)
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);

            try
            {
                if (await operation(attempt, cancellationToken).ConfigureAwait(false))
                    return true;

                onFailure?.Invoke(attempt, null);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                onFailure?.Invoke(attempt, ex);
                logger?.LogDebug(ex, "Attempt {Attempt} of {MaxAttempts} failed.", attempt, _maxAttempts);
            }
        }

        return false;
    }
}
