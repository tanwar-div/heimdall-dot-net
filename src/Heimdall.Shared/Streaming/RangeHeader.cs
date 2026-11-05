using System.Globalization;

namespace Heimdall.Shared.Streaming;

public enum RangeParseStatus
{
    /// <summary>No <c>Range</c> header: serve the whole object with 200.</summary>
    None,

    /// <summary>A single satisfiable range: serve 206 with <c>Content-Range</c>.</summary>
    Satisfiable,

    /// <summary>Syntactically valid but entirely past the end of the object: 416.</summary>
    Unsatisfiable,

    /// <summary>Unparseable or an unsupported unit. Per RFC 9110 the header is ignored: serve 200.</summary>
    Malformed,

    /// <summary>Multiple ranges. Heimdall does not emit multipart/byteranges, so it serves 200.</summary>
    MultipleRanges,
}

public readonly record struct RangeParseResult(RangeParseStatus Status, ByteRange Range)
{
    public bool IsPartial => Status == RangeParseStatus.Satisfiable;
}

/// <summary>
/// Parser for the single-range forms of the HTTP <c>Range</c> header, which is all a video player
/// ever sends while seeking: <c>bytes=0-</c>, <c>bytes=500-999</c>, and the suffix form
/// <c>bytes=-1024</c> ("the last 1024 bytes").
/// </summary>
public static class RangeHeader
{
    private const string BytesUnit = "bytes=";

    public static RangeParseResult Parse(string? headerValue, long totalLength)
    {
        if (string.IsNullOrWhiteSpace(headerValue))
            return new RangeParseResult(RangeParseStatus.None, default);

        var value = headerValue.Trim();
        if (!value.StartsWith(BytesUnit, StringComparison.OrdinalIgnoreCase))
            return new RangeParseResult(RangeParseStatus.Malformed, default);

        var spec = value[BytesUnit.Length..].Trim();
        if (spec.Length == 0)
            return new RangeParseResult(RangeParseStatus.Malformed, default);

        if (spec.Contains(','))
            return new RangeParseResult(RangeParseStatus.MultipleRanges, default);

        var dash = spec.IndexOf('-');
        if (dash < 0)
            return new RangeParseResult(RangeParseStatus.Malformed, default);

        var rawFrom = spec[..dash].Trim();
        var rawTo = spec[(dash + 1)..].Trim();

        // An empty object can satisfy no range at all.
        if (totalLength <= 0)
            return new RangeParseResult(RangeParseStatus.Unsatisfiable, default);

        if (rawFrom.Length == 0)
        {
            // Suffix form: bytes=-N means the final N bytes.
            if (!TryParseLong(rawTo, out var suffix) || suffix <= 0)
                return new RangeParseResult(RangeParseStatus.Malformed, default);

            var length = Math.Min(suffix, totalLength);
            return new RangeParseResult(
                RangeParseStatus.Satisfiable,
                new ByteRange(totalLength - length, totalLength - 1));
        }

        if (!TryParseLong(rawFrom, out var from) || from < 0)
            return new RangeParseResult(RangeParseStatus.Malformed, default);

        if (from >= totalLength)
            return new RangeParseResult(RangeParseStatus.Unsatisfiable, default);

        long to;
        if (rawTo.Length == 0)
        {
            // Open-ended form: bytes=N- means "N to the end". This is what a player sends first.
            to = totalLength - 1;
        }
        else
        {
            if (!TryParseLong(rawTo, out to) || to < from)
                return new RangeParseResult(RangeParseStatus.Malformed, default);

            // A range that overshoots the end is clamped, not rejected.
            to = Math.Min(to, totalLength - 1);
        }

        return new RangeParseResult(RangeParseStatus.Satisfiable, new ByteRange(from, to));
    }

    private static bool TryParseLong(string text, out long value)
        => long.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out value);
}
