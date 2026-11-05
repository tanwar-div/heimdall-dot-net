using System.Globalization;

namespace Heimdall.Shared.Streaming;

/// <summary>
/// Parser for the <c>Content-Range</c> *response* header, the mirror of
/// <see cref="RangeHeader"/>: <c>bytes 1000-1999/30000000</c>.
///
/// The gateway needs this to know the absolute file offset of a body it is relaying. Without it, a
/// transfer that breaks halfway cannot be resumed from another node, because "halfway" is only
/// meaningful as an absolute position in the original file.
/// </summary>
public static class ContentRange
{
    public readonly record struct Parsed(bool Success, long From, long To, long TotalLength);

    public static Parsed Parse(string? headerValue)
    {
        if (string.IsNullOrWhiteSpace(headerValue)) return default;

        var value = headerValue.Trim();
        if (!value.StartsWith("bytes", StringComparison.OrdinalIgnoreCase)) return default;

        var spec = value[5..].Trim();

        var slash = spec.LastIndexOf('/');
        if (slash <= 0) return default;

        var rangePart = spec[..slash].Trim();
        var totalPart = spec[(slash + 1)..].Trim();

        // "bytes */30000000" is the 416 form: a known length but no satisfied range.
        if (rangePart == "*") return default;

        var dash = rangePart.IndexOf('-');
        if (dash <= 0) return default;

        if (!long.TryParse(rangePart[..dash], NumberStyles.None, CultureInfo.InvariantCulture, out var from)
            || !long.TryParse(rangePart[(dash + 1)..], NumberStyles.None, CultureInfo.InvariantCulture, out var to)
            || to < from)
        {
            return default;
        }

        // An unknown total ("*") is legal; callers only need it for logging.
        var total = long.TryParse(totalPart, NumberStyles.None, CultureInfo.InvariantCulture, out var parsedTotal)
            ? parsedTotal
            : -1;

        return new Parsed(true, from, to, total);
    }
}
