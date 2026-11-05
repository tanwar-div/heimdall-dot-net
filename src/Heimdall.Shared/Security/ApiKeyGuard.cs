using System.Security.Cryptography;
using System.Text;
using Heimdall.Shared.Contracts;
using Microsoft.AspNetCore.Http;

namespace Heimdall.Shared.Security;

/// <summary>
/// Shared-secret check for write paths (upload, replication, delete). Reads stay open so a video
/// can be played by any client, which is the whole point of the service; anything that mutates the
/// cluster requires the key.
/// </summary>
public sealed class ApiKeyGuard
{
    private readonly byte[]? _expected;

    public ApiKeyGuard(string? apiKey)
    {
        _expected = string.IsNullOrEmpty(apiKey) ? null : Encoding.UTF8.GetBytes(apiKey);
    }

    /// <summary>False when no key is configured: writes are refused rather than left unauthenticated.</summary>
    public bool IsConfigured => _expected is not null;

    public bool IsAuthorized(HttpRequest request)
    {
        if (_expected is null) return false;

        if (!request.Headers.TryGetValue(HeimdallHeaders.ApiKey, out var provided) || provided.Count == 0)
            return false;

        var candidate = provided[0];
        if (string.IsNullOrEmpty(candidate)) return false;

        // Fixed-time comparison so a wrong key leaks nothing through response timing.
        return CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(candidate), _expected);
    }

    /// <summary>
    /// Writes an error response and returns false when the request is not authorized, so endpoints
    /// can start with <c>if (!guard.TryAuthorize(ctx, out var problem)) return problem;</c>.
    /// </summary>
    public bool TryAuthorize(HttpRequest request, out IResult failure)
    {
        if (!IsConfigured)
        {
            failure = Results.Problem(
                detail: "No Cluster:ApiKey is configured on this process, so write operations are disabled.",
                statusCode: StatusCodes.Status503ServiceUnavailable,
                title: "Write path not configured");
            return false;
        }

        if (!IsAuthorized(request))
        {
            failure = Results.Problem(
                detail: $"A valid {HeimdallHeaders.ApiKey} header is required for write operations.",
                statusCode: StatusCodes.Status401Unauthorized,
                title: "Unauthorized");
            return false;
        }

        failure = Results.Empty;
        return true;
    }
}
