using System.Security.Cryptography;
using Heimdall.Shared.Streaming;
using Heimdall.StorageNode;
using Heimdall.StorageNode.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Heimdall.Tests;

public class ChunkedVideoRepositoryTests
{
    private const long ChunkSize = 64 * 1024;

    private static (ChunkedVideoRepository Repository, InMemoryObjectStore Store) Build(
        long chunkSize = ChunkSize,
        string nodeId = "node-1")
    {
        var store = new InMemoryObjectStore();
        var options = Options.Create(new NodeOptions { Id = nodeId, ChunkSizeBytes = chunkSize });
        return (new ChunkedVideoRepository(store, options, NullLogger<ChunkedVideoRepository>.Instance), store);
    }

    private static byte[] RandomVideo(int length)
    {
        var bytes = new byte[length];
        new Random(1337).NextBytes(bytes);          // deterministic, so failures are reproducible
        return bytes;
    }

    private static async Task<byte[]> ReadRangeAsync(
        ChunkedVideoRepository repository,
        Shared.Contracts.VideoManifest manifest,
        ByteRange range)
    {
        using var sink = new MemoryStream();
        await repository.CopyRangeToAsync(manifest, range, sink);
        return sink.ToArray();
    }

    [Theory]
    [InlineData(0)]                     // empty upload
    [InlineData(1)]
    [InlineData(1024)]                  // smaller than one chunk
    [InlineData(64 * 1024)]             // exactly one chunk
    [InlineData(64 * 1024 + 1)]         // one byte into the second chunk
    [InlineData(3 * 64 * 1024)]         // an exact multiple of the chunk size
    [InlineData(5 * 64 * 1024 + 7777)]  // ragged tail
    public async Task Round_trip_is_byte_identical_at_every_chunk_boundary(int length)
    {
        var (repository, store) = Build();
        var source = RandomVideo(length);

        var manifest = await repository.IngestAsync(
            new VideoIngestRequest { VideoId = "clip", ContentType = "video/mp4", FileName = "clip.mp4" },
            new MemoryStream(source));

        Assert.Equal(length, manifest.Length);
        Assert.Equal(ChunkMap.ChunkCount(length, ChunkSize), manifest.ChunkCount);
        Assert.Equal("node-1", manifest.StoredOnNodeId);

        var downloaded = await ReadRangeAsync(repository, manifest, ByteRange.Full(length));
        Assert.Equal(source, downloaded);

        // Chunks plus exactly one manifest.
        Assert.Equal(manifest.ChunkCount + 1, store.Keys.Count);
        Assert.Contains("clip/manifest.json", store.Keys);
    }

    [Fact]
    public async Task Content_hash_is_the_sha256_of_the_upload()
    {
        // The manifest hash is what an integration test compares against the uploaded file, so it
        // has to be the plain SHA-256 of the bytes - not of the chunks, or of anything per-node.
        var (repository, _) = Build();
        var source = RandomVideo(200_000);

        var manifest = await repository.IngestAsync(
            new VideoIngestRequest { VideoId = "clip" }, new MemoryStream(source));

        Assert.Equal(Convert.ToHexStringLower(SHA256.HashData(source)), manifest.ContentHash);
        Assert.Equal($"\"{manifest.ContentHash}\"", manifest.ETag);
    }

    [Fact]
    public async Task Chunk_boundaries_are_exact_even_when_the_source_dribbles_bytes()
    {
        // A network stream returns short reads constantly. If ingest trusted a single ReadAsync it
        // would write undersized chunks, and every subsequent range calculation would be wrong.
        var (repository, store) = Build();
        var source = RandomVideo(3 * (int)ChunkSize + 12);

        var manifest = await repository.IngestAsync(
            new VideoIngestRequest { VideoId = "drip" },
            new DripStream(source, maxPerRead: 7));

        Assert.Equal(source.Length, manifest.Length);
        Assert.Equal(4, manifest.ChunkCount);

        Assert.Equal(ChunkSize, store["drip/chunk-00000"]!.Length);
        Assert.Equal(ChunkSize, store["drip/chunk-00001"]!.Length);
        Assert.Equal(ChunkSize, store["drip/chunk-00002"]!.Length);
        Assert.Equal(12, store["drip/chunk-00003"]!.Length);

        Assert.Equal(source, await ReadRangeAsync(repository, manifest, ByteRange.Full(manifest.Length)));
    }

    [Fact]
    public async Task Every_range_of_a_multi_chunk_video_matches_the_corresponding_source_slice()
    {
        var (repository, _) = Build(chunkSize: 1024);
        var source = RandomVideo(10_000);

        var manifest = await repository.IngestAsync(
            new VideoIngestRequest { VideoId = "clip" }, new MemoryStream(source));

        var ranges = new (long From, long To)[]
        {
            (0, 0),                 // first byte
            (9_999, 9_999),         // last byte
            (0, 1023),              // exactly chunk 0
            (1024, 2047),           // exactly chunk 1
            (1000, 1100),           // straddles a boundary
            (500, 5_500),           // spans five chunks
            (9_216, 9_999),         // the short final chunk
            (4_095, 4_096),         // the two bytes either side of a boundary
            (0, 9_999),             // everything
        };

        foreach (var (from, to) in ranges)
        {
            var actual = await ReadRangeAsync(repository, manifest, new ByteRange(from, to));
            Assert.Equal(source.AsSpan((int)from, (int)(to - from + 1)).ToArray(), actual);
        }
    }

    [Fact]
    public async Task A_seek_near_the_end_reads_only_the_chunks_it_needs()
    {
        // The point of chunking: serving the tail of a large video must not touch the whole object.
        var (repository, store) = Build(chunkSize: 1024);
        var manifest = await repository.IngestAsync(
            new VideoIngestRequest { VideoId = "clip" }, new MemoryStream(RandomVideo(1_024_000)));

        Assert.Equal(1000, manifest.ChunkCount);

        var before = store.RangeReadCount;
        await ReadRangeAsync(repository, manifest, new ByteRange(1_023_000, 1_023_999));

        // One chunk's worth of reads, not a thousand.
        Assert.Equal(1, store.RangeReadCount - before);
    }

    [Fact]
    public async Task OpenRangeStream_yields_the_same_bytes_as_a_direct_copy()
    {
        // This is the stream the replication path hands to HttpClient, so it must agree with the
        // read path byte for byte and report a Content-Length up front.
        var (repository, _) = Build(chunkSize: 4096);
        var source = RandomVideo(20_000);

        var manifest = await repository.IngestAsync(
            new VideoIngestRequest { VideoId = "clip" }, new MemoryStream(source));

        await using var stream = repository.OpenFullStream(manifest);
        Assert.Equal(20_000, stream.Length);

        using var sink = new MemoryStream();
        await stream.CopyToAsync(sink);

        Assert.Equal(source, sink.ToArray());
    }

    [Fact]
    public async Task OpenRangeStream_honours_a_partial_range()
    {
        var (repository, _) = Build(chunkSize: 1024);
        var source = RandomVideo(10_000);

        var manifest = await repository.IngestAsync(
            new VideoIngestRequest { VideoId = "clip" }, new MemoryStream(source));

        await using var stream = repository.OpenRangeStream(manifest, new ByteRange(1_500, 3_499));
        Assert.Equal(2_000, stream.Length);

        using var sink = new MemoryStream();
        await stream.CopyToAsync(sink);

        Assert.Equal(source.AsSpan(1_500, 2_000).ToArray(), sink.ToArray());
    }

    [Fact]
    public async Task Manifest_round_trips_through_object_storage()
    {
        var (repository, _) = Build();

        var written = await repository.IngestAsync(
            new VideoIngestRequest
            {
                VideoId = "clip",
                ContentType = "video/webm",
                FileName = "holiday.webm",
                OwnerNodeId = "node-4",
                IsReplica = true,
            },
            new MemoryStream(RandomVideo(5_000)));

        var read = await repository.GetManifestAsync("clip");

        Assert.NotNull(read);
        Assert.Equal(written.ContentHash, read.ContentHash);
        Assert.Equal("video/webm", read.ContentType);
        Assert.Equal("holiday.webm", read.FileName);
        Assert.Equal("node-4", read.OwnerNodeId);       // the ring owner, asserted by the primary
        Assert.Equal("node-1", read.StoredOnNodeId);    // where this copy physically lives
        Assert.True(read.IsReplica);
        Assert.Equal(ChunkSize, read.ChunkSize);
    }

    [Fact]
    public async Task Reading_an_unknown_video_returns_no_manifest()
        => Assert.Null(await Build().Repository.GetManifestAsync("never-uploaded"));

    [Fact]
    public async Task Delete_removes_the_manifest_and_every_chunk()
    {
        var (repository, store) = Build(chunkSize: 1024);
        await repository.IngestAsync(new VideoIngestRequest { VideoId = "clip" }, new MemoryStream(RandomVideo(5_000)));

        Assert.Equal(6, store.Keys.Count);               // 5 chunks + manifest

        var deleted = await repository.DeleteAsync("clip");

        Assert.Equal(6, deleted);
        Assert.Empty(store.Keys);
        Assert.Null(await repository.GetManifestAsync("clip"));
    }

    [Fact]
    public async Task Delete_is_scoped_to_one_video()
    {
        var (repository, store) = Build(chunkSize: 1024);
        await repository.IngestAsync(new VideoIngestRequest { VideoId = "clip" }, new MemoryStream(RandomVideo(2_000)));
        await repository.IngestAsync(new VideoIngestRequest { VideoId = "clip-two" }, new MemoryStream(RandomVideo(2_000)));

        await repository.DeleteAsync("clip");

        Assert.All(store.Keys, key => Assert.StartsWith("clip-two/", key));
        Assert.NotNull(await repository.GetManifestAsync("clip-two"));
    }

    [Fact]
    public async Task Re_ingesting_the_same_id_overwrites_rather_than_appending()
    {
        var (repository, store) = Build(chunkSize: 1024);

        await repository.IngestAsync(new VideoIngestRequest { VideoId = "clip" }, new MemoryStream(RandomVideo(5_000)));
        var replacement = RandomVideo(1_500);
        var manifest = await repository.IngestAsync(new VideoIngestRequest { VideoId = "clip" }, new MemoryStream(replacement));

        Assert.Equal(1_500, manifest.Length);
        Assert.Equal(replacement, await ReadRangeAsync(repository, manifest, ByteRange.Full(1_500)));

        // Note the stale chunks from the longer first upload: overwriting a video with a shorter
        // one leaves orphans behind. They are unreachable (the manifest bounds every read) but a
        // production deployment would want a sweeper for them.
        Assert.Contains("clip/chunk-00004", store.Keys);
    }

    [Fact]
    public async Task ReadAtLeastAsync_fills_the_buffer_across_short_reads_and_reports_the_tail()
    {
        var source = RandomVideo(25);
        var stream = new DripStream(source, maxPerRead: 4);

        var buffer = new byte[10];
        Assert.Equal(10, await ChunkedVideoRepository.ReadAtLeastAsync(stream, buffer, default));
        Assert.Equal(source[..10], buffer);

        Assert.Equal(10, await ChunkedVideoRepository.ReadAtLeastAsync(stream, buffer, default));
        Assert.Equal(5, await ChunkedVideoRepository.ReadAtLeastAsync(stream, buffer, default));   // tail
        Assert.Equal(0, await ChunkedVideoRepository.ReadAtLeastAsync(stream, buffer, default));   // EOF
    }
}
