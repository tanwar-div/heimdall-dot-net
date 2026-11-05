namespace Heimdall.Shared.Streaming;

/// <summary>
/// An inclusive byte range, exactly as HTTP defines it: <c>bytes=0-1</c> is two bytes, not one.
/// </summary>
public readonly record struct ByteRange
{
    public ByteRange(long from, long to)
    {
        if (from < 0) throw new ArgumentOutOfRangeException(nameof(from), "Range start cannot be negative.");
        if (to < from) throw new ArgumentOutOfRangeException(nameof(to), "Range end cannot precede range start.");
        From = from;
        To = to;
    }

    /// <summary>First byte of the range, inclusive.</summary>
    public long From { get; }

    /// <summary>Last byte of the range, inclusive.</summary>
    public long To { get; }

    /// <summary>Number of bytes covered. Inclusive bounds, hence the +1.</summary>
    public long Length => To - From + 1;

    /// <summary>The whole of a <paramref name="totalLength"/>-byte object.</summary>
    public static ByteRange Full(long totalLength) => new(0, Math.Max(0, totalLength - 1));

    /// <summary>Value for a <c>Content-Range</c> response header.</summary>
    public string ToContentRange(long totalLength) => $"bytes {From}-{To}/{totalLength}";

    /// <summary>Value for a <c>Range</c> request header.</summary>
    public string ToRangeHeader() => $"bytes={From}-{To}";

    public override string ToString() => $"[{From}..{To}] ({Length} bytes)";
}
