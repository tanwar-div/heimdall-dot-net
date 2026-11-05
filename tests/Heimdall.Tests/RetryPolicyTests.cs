using Heimdall.Shared.Http;

namespace Heimdall.Tests;

public class RetryPolicyTests
{
    [Fact]
    public async Task Succeeds_without_retrying_when_the_first_attempt_works()
    {
        var attempts = 0;
        var policy = new RetryPolicy(maxAttempts: 4, baseDelay: TimeSpan.Zero, maxDelay: TimeSpan.Zero);

        var ok = await policy.ExecuteAsync((_, _) => { attempts++; return Task.FromResult(true); });

        Assert.True(ok);
        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task Retries_a_failing_operation_up_to_the_limit_then_gives_up()
    {
        var attempts = 0;
        var failures = new List<int>();
        var policy = new RetryPolicy(maxAttempts: 3, baseDelay: TimeSpan.Zero, maxDelay: TimeSpan.Zero);

        var ok = await policy.ExecuteAsync(
            (_, _) => { attempts++; return Task.FromResult(false); },
            onFailure: (attempt, _) => failures.Add(attempt));

        Assert.False(ok);
        Assert.Equal(3, attempts);
        Assert.Equal([1, 2, 3], failures);
    }

    [Fact]
    public async Task An_exception_is_a_retryable_failure_and_is_surfaced_to_the_caller()
    {
        // A replica that refuses a connection throws rather than returning a status, and must be
        // retried exactly like a 500 would be.
        var attempts = 0;
        var seen = new List<Exception?>();
        var policy = new RetryPolicy(maxAttempts: 3, baseDelay: TimeSpan.Zero, maxDelay: TimeSpan.Zero);

        var ok = await policy.ExecuteAsync(
            (attempt, _) =>
            {
                attempts++;
                if (attempt < 3) throw new HttpRequestException("connection refused");
                return Task.FromResult(true);
            },
            onFailure: (_, ex) => seen.Add(ex));

        Assert.True(ok);
        Assert.Equal(3, attempts);
        Assert.Equal(2, seen.Count);
        Assert.All(seen, ex => Assert.IsType<HttpRequestException>(ex));
    }

    [Fact]
    public void The_first_attempt_is_immediate_and_later_delays_are_bounded_and_jittered()
    {
        var policy = new RetryPolicy(maxAttempts: 10, baseDelay: TimeSpan.FromSeconds(2), maxDelay: TimeSpan.FromSeconds(30));

        Assert.Equal(TimeSpan.Zero, policy.DelayFor(1));

        // Full jitter: each delay is somewhere in [0, cap], and the cap doubles until maxDelay.
        for (var attempt = 2; attempt <= 10; attempt++)
        {
            var cap = Math.Min(2000 * Math.Pow(2, attempt - 2), 30_000);
            for (var sample = 0; sample < 50; sample++)
                Assert.InRange(policy.DelayFor(attempt).TotalMilliseconds, 0, cap);
        }
    }

    [Fact]
    public void Jitter_actually_varies_so_replicas_do_not_retry_in_lockstep()
    {
        var policy = new RetryPolicy(baseDelay: TimeSpan.FromSeconds(5));
        var samples = Enumerable.Range(0, 50).Select(_ => policy.DelayFor(4)).Distinct().Count();

        Assert.True(samples > 10, $"Only {samples} distinct delays in 50 samples - jitter is not working.");
    }

    [Fact]
    public async Task Cancellation_propagates_instead_of_being_swallowed_as_a_failure()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var policy = new RetryPolicy(maxAttempts: 3, baseDelay: TimeSpan.Zero, maxDelay: TimeSpan.Zero);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => policy.ExecuteAsync(
            (_, token) => { token.ThrowIfCancellationRequested(); return Task.FromResult(true); },
            cancellationToken: cts.Token));
    }

    [Fact]
    public void Rejects_a_zero_attempt_budget()
        => Assert.Throws<ArgumentOutOfRangeException>(() => new RetryPolicy(maxAttempts: 0));
}
