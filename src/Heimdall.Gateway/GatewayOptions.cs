namespace Heimdall.Gateway;

/// <summary>Gateway-specific settings, bound from the <c>Gateway</c> configuration section.</summary>
public sealed class GatewayOptions
{
    public const string SectionName = "Gateway";

    /// <summary>
    /// Public base URL used to build the <c>streamUrl</c> in an upload response. When unset the
    /// gateway derives it from the request, which is usually right behind a single proxy and wrong
    /// behind two.
    /// </summary>
    public string? PublicBaseUrl { get; set; }

    /// <summary>
    /// Give up on a read candidate that has not produced response *headers* within this window and
    /// move to the next one. It does not bound the body transfer, which can legitimately take
    /// hours on a paused video.
    /// </summary>
    public TimeSpan CandidateHeaderTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Abandon a node that goes silent for this long mid-body and resume the response from another
    /// holder.
    ///
    /// This is not belt-and-braces: a node that is SIGKILLed or partitioned away sends no RST, so a
    /// blocking socket read against it never returns. Without an idle timeout the gateway would
    /// hang on the dead peer forever and the resume path could never fire. It bounds only the wait
    /// for *upstream* bytes - a paused player that stops reading is parked in a write, not a read,
    /// so playback can idle indefinitely without tripping this.
    /// </summary>
    public TimeSpan RelayIdleTimeout { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>Buffer used when relaying a node's body to the client.</summary>
    public int ProxyBufferBytes { get; set; } = 128 * 1024;

    public void Validate()
    {
        if (CandidateHeaderTimeout <= TimeSpan.Zero)
            throw new InvalidOperationException("Gateway:CandidateHeaderTimeout must be positive.");
        if (RelayIdleTimeout <= TimeSpan.Zero)
            throw new InvalidOperationException("Gateway:RelayIdleTimeout must be positive.");
        if (ProxyBufferBytes < 4096)
            throw new InvalidOperationException("Gateway:ProxyBufferBytes must be at least 4096.");
        // A JSON null and an unset environment variable both bind to an empty string, so "unset"
        // has to mean null-or-whitespace rather than just null.
        if (!string.IsNullOrWhiteSpace(PublicBaseUrl) && !Uri.TryCreate(PublicBaseUrl, UriKind.Absolute, out _))
            throw new InvalidOperationException($"Gateway:PublicBaseUrl is not a valid absolute URI: '{PublicBaseUrl}'.");
    }
}
