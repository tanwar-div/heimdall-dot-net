using System.Buffers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Heimdall.Shared.Contracts;
using Heimdall.Shared.Streaming;
using Microsoft.Extensions.Options;

namespace Heimdall.StorageNode.Storage;

/// <summary>What the node is being asked to store, independent of how the bytes arrived.</summary>
public sealed record VideoIngestRequest
{
    public required string VideoId { get; init; }
    public string ContentType { get; init; } = "application/octet-stream";
    public string? FileName { get; init; }

    /// <summary>Ring primary for this id, as asserted by the writer. Defaults to this node.</summary>
    public string? OwnerNodeId { get; init; }

    /// <summary>True when the write is replication traffic from the primary rather than a client upload.</summary>
    public bool IsReplica { get; init; }
}

/// <summary>
/// Stores videos as a sequence of fixed-size chunks plus a manifest, and serves arbitrary byte
/// ranges back out of them.
///
/// Ingest is streaming: the source is read one chunk-sized buffer at a time and each buffer is
/// pushed straight to object storage, so peak memory is one chunk (8 MiB by default) no matter how
/// large the upload. A rolling SHA-256 is computed on the way through and lands in the manifest,
/// which is what lets an integration test assert that a download is byte-identical to the upload.
///
/// The manifest is written last, on purpose: its presence is the commit marker. A node that dies
/// mid-ingest leaves orphaned chunks and no manifest, and reads for that id return 404 rather than
/// a truncated video.
/// </summary>
public sealed class ChunkedVideoRepository
{
    private static readonly JsonSerializerOptions ManifestJson = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = false,
    };

    private readonly IObjectStore _store;
    private readonly NodeOptions _node;
    private readonly ILogger<ChunkedVideoRepository> _logger;

    public ChunkedVideoRepository(
        IObjectStore store,
        IOptions<NodeOptions> node,
        ILogger<ChunkedVideoRepository> logger)
    {
        _store = store;
        _node = node.Value;
        _logger = logger;
    }

    public long ChunkSize => _node.ChunkSizeBytes;

    public async Task<VideoManifest> IngestAsync(
        VideoIngestRequest request,
        Stream source,
        CancellationToken cancellationToken = default)
    {
        var chunkSize = _node.ChunkSizeBytes;
        var buffer = ArrayPool<byte>.Shared.Rent((int)chunkSize);
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

        long total = 0;
        var index = 0;

        try
        {
            while (true)
            {
                // Fill a full chunk before writing: a Stream.Read may return short even mid-file,
                // and chunk boundaries have to be exact for the range math to hold.
                var filled = await ReadAtLeastAsync(source, buffer.AsMemory(0, (int)chunkSize), cancellationToken)
                    .ConfigureAwait(false);

                if (filled == 0) break;

                hasher.AppendData(buffer, 0, filled);

                // A MemoryStream over the pooled buffer: the S3 PUT needs a seekable body with a
                // known length, and this is the one place a copy is unavoidable.
                using var chunkBody = new MemoryStream(buffer, 0, filled, writable: false, publiclyVisible: true);
                await _store
                    .PutAsync(ChunkMap.ChunkKey(request.VideoId, index), chunkBody, filled, "application/octet-stream", cancellationToken)
                    .ConfigureAwait(false);

                total += filled;
                index++;

                if (filled < chunkSize) break;   // short read with no bytes left means end of stream
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        var manifest = new VideoManifest
        {
            VideoId = request.VideoId,
            Length = total,
            ChunkSize = chunkSize,
            ChunkCount = index,
            ContentType = request.ContentType,
            FileName = request.FileName,
            ContentHash = Convert.ToHexStringLower(hasher.GetHashAndReset()),
            OwnerNodeId = request.OwnerNodeId ?? _node.Id,
            StoredOnNodeId = _node.Id,
            IsReplica = request.IsReplica,
            CreatedAt = DateTimeOffset.UtcNow,
        };

        await WriteManifestAsync(manifest, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation(
            "Ingested {VideoId} on {NodeId}: {Length} bytes in {ChunkCount} chunks (replica={IsReplica}).",
            manifest.VideoId, _node.Id, manifest.Length, manifest.ChunkCount, manifest.IsReplica);

        return manifest;
    }

    public async Task<VideoManifest?> GetManifestAsync(string videoId, CancellationToken cancellationToken = default)
    {
        var json = await _store.GetTextAsync(ChunkMap.ManifestKey(videoId), cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrEmpty(json)) return null;

        try
        {
            return JsonSerializer.Deserialize<VideoManifest>(json, ManifestJson);
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "Manifest for {VideoId} on {NodeId} is corrupt.", videoId, _node.Id);
            return null;
        }
    }

    /// <summary>
    /// Copies <paramref name="range"/> straight into <paramref name="destination"/>, one chunk
    /// slice at a time. Nothing larger than the copy buffer is held in memory, and the HTTP
    /// response starts flowing as soon as the first chunk read returns.
    /// </summary>
    public async Task<long> CopyRangeToAsync(
        VideoManifest manifest,
        ByteRange range,
        Stream destination,
        CancellationToken cancellationToken = default)
    {
        long written = 0;

        foreach (var slice in ChunkMap.Slice(range, manifest.ChunkSize, manifest.Length))
        {
            cancellationToken.ThrowIfCancellationRequested();

            await using var chunk = await _store
                .OpenReadAsync(ChunkMap.ChunkKey(manifest.VideoId, slice.Index), slice.OffsetInChunk, slice.Length, cancellationToken)
                .ConfigureAwait(false);

            await chunk.CopyToAsync(destination, 81_920, cancellationToken).ConfigureAwait(false);
            written += slice.Length;
        }

        await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
        return written;
    }

    /// <summary>
    /// A lazily-read stream over a range, for callers that need a <see cref="Stream"/> rather than
    /// a copy target - notably the replication path, which hands it to <c>HttpClient</c> as a PUT body.
    /// </summary>
    public ChunkRangeStream OpenRangeStream(VideoManifest manifest, ByteRange range)
        => new(_store, manifest.VideoId, manifest.ChunkSize, manifest.Length, range);

    public ChunkRangeStream OpenFullStream(VideoManifest manifest)
        => OpenRangeStream(manifest, ByteRange.Full(manifest.Length));

    /// <summary>
    /// Removes the manifest first so the video stops being readable immediately, then the chunks.
    /// Returns the number of objects deleted.
    /// </summary>
    public async Task<int> DeleteAsync(string videoId, CancellationToken cancellationToken = default)
    {
        var deleted = await _store.DeletePrefixAsync(ChunkMap.ManifestKey(videoId), cancellationToken).ConfigureAwait(false);
        deleted += await _store.DeletePrefixAsync($"{videoId}/", cancellationToken).ConfigureAwait(false);
        return deleted;
    }

    private async Task WriteManifestAsync(VideoManifest manifest, CancellationToken cancellationToken)
    {
        var bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(manifest, ManifestJson));
        using var body = new MemoryStream(bytes, writable: false);
        await _store
            .PutAsync(ChunkMap.ManifestKey(manifest.VideoId), body, bytes.Length, "application/json", cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Reads until <paramref name="buffer"/> is full or the source ends, returning the byte count.
    /// <see cref="Stream.ReadAsync(Memory{byte}, CancellationToken)"/> is allowed to return fewer
    /// bytes than asked for at any point, so a single call is never enough to fill a chunk.
    /// </summary>
    internal static async ValueTask<int> ReadAtLeastAsync(
        Stream source,
        Memory<byte> buffer,
        CancellationToken cancellationToken)
    {
        var filled = 0;
        while (filled < buffer.Length)
        {
            var read = await source.ReadAsync(buffer[filled..], cancellationToken).ConfigureAwait(false);
            if (read == 0) break;
            filled += read;
        }
        return filled;
    }
}
