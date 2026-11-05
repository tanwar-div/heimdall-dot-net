using Heimdall.Shared.Streaming;

namespace Heimdall.Tests;

public class RangeHeaderTests
{
    private const long OneGibibyte = 1024L * 1024 * 1024;

    [Fact]
    public void No_header_means_serve_the_whole_object()
    {
        Assert.Equal(RangeParseStatus.None, RangeHeader.Parse(null, 1000).Status);
        Assert.Equal(RangeParseStatus.None, RangeHeader.Parse("", 1000).Status);
        Assert.Equal(RangeParseStatus.None, RangeHeader.Parse("   ", 1000).Status);
    }

    [Fact]
    public void Open_ended_range_runs_to_the_last_byte()
    {
        // This is the first request a <video> element makes.
        var result = RangeHeader.Parse("bytes=0-", 1000);

        Assert.True(result.IsPartial);
        Assert.Equal(0, result.Range.From);
        Assert.Equal(999, result.Range.To);
        Assert.Equal(1000, result.Range.Length);
    }

    [Fact]
    public void Closed_range_is_inclusive_on_both_ends()
    {
        var result = RangeHeader.Parse("bytes=500-999", 1000);

        Assert.Equal(500, result.Range.From);
        Assert.Equal(999, result.Range.To);
        Assert.Equal(500, result.Range.Length);     // not 499: HTTP ranges are inclusive
    }

    [Fact]
    public void Single_byte_range_has_length_one()
    {
        var result = RangeHeader.Parse("bytes=42-42", 1000);

        Assert.True(result.IsPartial);
        Assert.Equal(1, result.Range.Length);
    }

    [Fact]
    public void Suffix_range_returns_the_tail()
    {
        // A player looking for the moov atom in a badly muxed MP4 asks for the end of the file.
        var result = RangeHeader.Parse("bytes=-500", 1000);

        Assert.Equal(500, result.Range.From);
        Assert.Equal(999, result.Range.To);
        Assert.Equal(500, result.Range.Length);
    }

    [Fact]
    public void Suffix_longer_than_the_object_is_clamped_to_the_whole_object()
    {
        var result = RangeHeader.Parse("bytes=-5000", 1000);

        Assert.Equal(0, result.Range.From);
        Assert.Equal(999, result.Range.To);
    }

    [Fact]
    public void End_past_the_object_is_clamped_rather_than_rejected()
    {
        var result = RangeHeader.Parse("bytes=900-9999", 1000);

        Assert.True(result.IsPartial);
        Assert.Equal(900, result.Range.From);
        Assert.Equal(999, result.Range.To);
        Assert.Equal(100, result.Range.Length);
    }

    [Fact]
    public void Start_past_the_end_is_unsatisfiable()
    {
        Assert.Equal(RangeParseStatus.Unsatisfiable, RangeHeader.Parse("bytes=1000-", 1000).Status);
        Assert.Equal(RangeParseStatus.Unsatisfiable, RangeHeader.Parse("bytes=5000-6000", 1000).Status);
    }

    [Fact]
    public void An_empty_object_can_satisfy_no_range()
        => Assert.Equal(RangeParseStatus.Unsatisfiable, RangeHeader.Parse("bytes=0-", 0).Status);

    [Theory]
    [InlineData("items=0-10")]      // wrong unit
    [InlineData("bytes=")]
    [InlineData("bytes=abc-def")]
    [InlineData("bytes=10")]        // no dash
    [InlineData("bytes=-")]
    [InlineData("bytes=-0")]        // a zero-length suffix is meaningless
    [InlineData("bytes=900-100")]   // end before start
    [InlineData("bytes=-10-20")]
    public void Malformed_headers_are_reported_as_malformed_so_the_caller_serves_200(string header)
        => Assert.Equal(RangeParseStatus.Malformed, RangeHeader.Parse(header, 1000).Status);

    [Fact]
    public void Multiple_ranges_are_detected_rather_than_mis_parsed()
    {
        // Heimdall does not emit multipart/byteranges; callers fall back to a full 200 response.
        Assert.Equal(RangeParseStatus.MultipleRanges, RangeHeader.Parse("bytes=0-50,100-150", 1000).Status);
    }

    [Fact]
    public void Unit_and_whitespace_are_handled_leniently()
    {
        Assert.True(RangeHeader.Parse("BYTES=0-10", 1000).IsPartial);
        Assert.True(RangeHeader.Parse("  bytes= 0 - 10 ", 1000).IsPartial);
    }

    [Fact]
    public void Multi_gigabyte_offsets_stay_exact()
    {
        // 32-bit arithmetic anywhere on this path would silently corrupt seeks in large files.
        var total = 4 * OneGibibyte + 12_345;
        var result = RangeHeader.Parse($"bytes={3 * OneGibibyte}-{3 * OneGibibyte + 1023}", total);

        Assert.True(result.IsPartial);
        Assert.Equal(3 * OneGibibyte, result.Range.From);
        Assert.Equal(1024, result.Range.Length);
        Assert.Equal($"bytes {3 * OneGibibyte}-{3 * OneGibibyte + 1023}/{total}",
            result.Range.ToContentRange(total));
    }

    [Fact]
    public void ByteRange_rejects_impossible_bounds()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ByteRange(-1, 10));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ByteRange(10, 9));
    }

    [Fact]
    public void Full_range_covers_the_object_and_survives_a_zero_length_object()
    {
        Assert.Equal(1000, ByteRange.Full(1000).Length);
        Assert.Equal(0, ByteRange.Full(1000).From);
        Assert.Equal(999, ByteRange.Full(1000).To);

        // Length 0 must not produce a negative-length range.
        Assert.Equal(0, ByteRange.Full(0).From);
        Assert.Equal(0, ByteRange.Full(0).To);
    }

    [Fact]
    public void Round_trips_through_the_request_header_form()
    {
        var original = new ByteRange(1_000, 2_047);
        var reparsed = RangeHeader.Parse(original.ToRangeHeader(), 10_000);

        Assert.Equal(original, reparsed.Range);
    }
}
