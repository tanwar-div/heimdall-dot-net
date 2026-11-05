namespace Heimdall.Shared.Streaming;

/// <summary>
/// The part of one stored chunk that a byte range needs: chunk <paramref name="Index"/>, starting
/// <paramref name="OffsetInChunk"/> bytes into it, for <paramref name="Length"/> bytes.
/// </summary>
public readonly record struct ChunkSlice(int Index, long OffsetInChunk, long Length)
{
    /// <summary>True when the slice covers the chunk from its first byte to its last.</summary>
    public bool IsWholeChunk(long chunkSize, long totalLength)
    {
        var chunkLength = ChunkMap.ChunkLength(Index, chunkSize, totalLength);
        return OffsetInChunk == 0 && Length == chunkLength;
    }
}

/// <summary>
/// Maps absolute byte ranges onto fixed-size chunks.
///
/// Videos are stored as <c>chunk-00000</c>, <c>chunk-00001</c>, ... of <c>ChunkSize</c> bytes each
/// (the last one short). Serving <c>Range: bytes=734003200-734527487</c> of a 4 GB file then means
/// fetching two or three chunks and trimming the ends, instead of reading the whole object. This is
/// the piece that makes seeking cheap and keeps memory flat regardless of file size.
/// </summary>
public static class ChunkMap
{
    public const long DefaultChunkSize = 8L * 1024 * 1024;

    /// <summary>Number of chunks a <paramref name="totalLength"/>-byte object occupies.</summary>
    public static int ChunkCount(long totalLength, long chunkSize)
    {
        ValidateChunkSize(chunkSize);
        if (totalLength <= 0) return 0;
        return (int)((totalLength + chunkSize - 1) / chunkSize);
    }

    /// <summary>Stored size of chunk <paramref name="index"/>; the final chunk is usually short.</summary>
    public static long ChunkLength(int index, long chunkSize, long totalLength)
    {
        ValidateChunkSize(chunkSize);
        if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));

        var start = (long)index * chunkSize;
        if (start >= totalLength) return 0;
        return Math.Min(chunkSize, totalLength - start);
    }

    /// <summary>Object name for a chunk. Zero-padded so a bucket listing sorts in read order.</summary>
    public static string ChunkKey(string videoId, int index) => $"{videoId}/chunk-{index:D5}";

    /// <summary>Object name for a video's manifest.</summary>
    public static string ManifestKey(string videoId) => $"{videoId}/manifest.json";

    /// <summary>
    /// The chunk slices that together cover <paramref name="range"/>, in read order. Lazily
    /// enumerated: the caller streams each slice to the client as it goes and never holds more than
    /// one buffer at a time.
    /// </summary>
    public static IEnumerable<ChunkSlice> Slice(ByteRange range, long chunkSize, long totalLength)
    {
        ValidateChunkSize(chunkSize);
        if (totalLength <= 0 || range.From >= totalLength) yield break;

        var from = range.From;
        var to = Math.Min(range.To, totalLength - 1);

        var index = (int)(from / chunkSize);
        var cursor = from;

        while (cursor <= to)
        {
            var chunkStart = (long)index * chunkSize;
            var offsetInChunk = cursor - chunkStart;
            var chunkLength = ChunkLength(index, chunkSize, totalLength);
            var available = chunkLength - offsetInChunk;
            var needed = to - cursor + 1;
            var take = Math.Min(available, needed);

            if (take <= 0) yield break;   // defensive: a truncated object cannot satisfy the range

            yield return new ChunkSlice(index, offsetInChunk, take);

            cursor += take;
            index++;
        }
    }

    private static void ValidateChunkSize(long chunkSize)
    {
        if (chunkSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(chunkSize), "Chunk size must be greater than zero.");
    }
}
