using Heimdall.Shared.Streaming;

namespace Heimdall.Tests;

public class ChunkMapTests
{
    [Theory]
    [InlineData(0, 100, 0)]
    [InlineData(1, 100, 1)]
    [InlineData(100, 100, 1)]
    [InlineData(101, 100, 2)]
    [InlineData(1000, 100, 10)]
    [InlineData(1001, 100, 11)]
    public void Chunk_count_rounds_up(long total, long chunkSize, int expected)
        => Assert.Equal(expected, ChunkMap.ChunkCount(total, chunkSize));

    [Fact]
    public void Last_chunk_is_short_and_chunks_past_the_end_are_empty()
    {
        Assert.Equal(100, ChunkMap.ChunkLength(0, 100, 250));
        Assert.Equal(100, ChunkMap.ChunkLength(1, 100, 250));
        Assert.Equal(50, ChunkMap.ChunkLength(2, 100, 250));
        Assert.Equal(0, ChunkMap.ChunkLength(3, 100, 250));
    }

    [Fact]
    public void Chunk_keys_are_zero_padded_so_a_bucket_listing_sorts_in_read_order()
    {
        Assert.Equal("vid/chunk-00000", ChunkMap.ChunkKey("vid", 0));
        Assert.Equal("vid/chunk-00042", ChunkMap.ChunkKey("vid", 42));
        Assert.Equal("vid/chunk-12345", ChunkMap.ChunkKey("vid", 12345));
        Assert.Equal("vid/manifest.json", ChunkMap.ManifestKey("vid"));
    }

    [Fact]
    public void A_range_inside_one_chunk_produces_one_slice()
    {
        var slices = ChunkMap.Slice(new ByteRange(10, 19), chunkSize: 100, totalLength: 1000).ToArray();

        Assert.Single(slices);
        Assert.Equal(new ChunkSlice(0, 10, 10), slices[0]);
    }

    [Fact]
    public void A_range_spanning_chunks_is_split_at_the_boundaries()
    {
        // bytes 90..219 of 100-byte chunks: the tail of chunk 0, all of chunk 1, the head of chunk 2.
        var slices = ChunkMap.Slice(new ByteRange(90, 219), chunkSize: 100, totalLength: 1000).ToArray();

        Assert.Equal(
        [
            new ChunkSlice(0, 90, 10),
            new ChunkSlice(1, 0, 100),
            new ChunkSlice(2, 0, 20),
        ], slices);
    }

    [Fact]
    public void A_chunk_aligned_range_produces_whole_chunk_slices()
    {
        var slices = ChunkMap.Slice(new ByteRange(100, 299), chunkSize: 100, totalLength: 1000).ToArray();

        Assert.Equal(2, slices.Length);
        Assert.All(slices, s => Assert.True(s.IsWholeChunk(100, 1000)));
    }

    [Fact]
    public void The_final_slice_stops_at_the_real_end_of_the_object()
    {
        // 250 bytes in 100-byte chunks: asking for "everything from 200" must yield 50, not 100.
        var slices = ChunkMap.Slice(new ByteRange(200, 299), chunkSize: 100, totalLength: 250).ToArray();

        Assert.Single(slices);
        Assert.Equal(new ChunkSlice(2, 0, 50), slices[0]);
    }

    [Fact]
    public void Slices_are_empty_for_an_empty_object_or_a_range_past_the_end()
    {
        Assert.Empty(ChunkMap.Slice(new ByteRange(0, 99), 100, 0));
        Assert.Empty(ChunkMap.Slice(new ByteRange(500, 599), 100, 250));
    }

    [Fact]
    public void Slices_are_contiguous_and_sum_to_the_requested_length_for_every_range()
    {
        // Exhaustive over a small object: every (from, to) pair must decompose into adjacent slices
        // whose lengths add up exactly. Off-by-one errors here would corrupt playback mid-seek.
        const long chunkSize = 7;
        const long total = 50;

        for (long from = 0; from < total; from++)
        {
            for (var to = from; to < total; to++)
            {
                var range = new ByteRange(from, to);
                var slices = ChunkMap.Slice(range, chunkSize, total).ToArray();

                Assert.Equal(range.Length, slices.Sum(s => s.Length));

                var cursor = from;
                foreach (var slice in slices)
                {
                    var absolute = slice.Index * chunkSize + slice.OffsetInChunk;
                    Assert.Equal(cursor, absolute);
                    Assert.True(slice.Length > 0);
                    Assert.True(slice.OffsetInChunk + slice.Length <= chunkSize);
                    cursor += slice.Length;
                }

                Assert.Equal(to + 1, cursor);
            }
        }
    }

    [Fact]
    public void Slicing_a_multi_gigabyte_object_uses_64_bit_arithmetic_throughout()
    {
        const long chunkSize = 8L * 1024 * 1024;
        const long total = 6L * 1024 * 1024 * 1024;       // 6 GiB
        var from = 5L * 1024 * 1024 * 1024;               // seek to the 5 GiB mark

        var slices = ChunkMap.Slice(new ByteRange(from, from + 1_000_000), chunkSize, total).ToArray();

        Assert.Equal(640, slices[0].Index);               // 5 GiB / 8 MiB
        Assert.Equal(0, slices[0].OffsetInChunk);
        Assert.Equal(1_000_001, slices.Sum(s => s.Length));
        Assert.Equal(768, ChunkMap.ChunkCount(total, chunkSize));
    }

    [Fact]
    public void Invalid_chunk_sizes_are_rejected()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => ChunkMap.ChunkCount(100, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => ChunkMap.ChunkLength(0, -1, 100));
        Assert.Throws<ArgumentOutOfRangeException>(() => ChunkMap.Slice(new ByteRange(0, 1), 0, 100).ToArray());
    }
}
