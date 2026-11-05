using Heimdall.Shared.Streaming;

namespace Heimdall.StorageNode.Storage;

/// <summary>
/// A forward-only read stream over the chunks that cover one byte range.
///
/// It opens each underlying chunk read only when the consumer actually asks for those bytes, so a
/// 4 GB video streamed to a replica (or to a client) never occupies more than one chunk read's
/// worth of in-flight buffer. <see cref="Length"/> is known up front, which lets the replication
/// path send a real <c>Content-Length</c> instead of falling back to chunked encoding.
/// </summary>
public sealed class ChunkRangeStream : Stream
{
    private readonly IObjectStore _store;
    private readonly string _videoId;
    private readonly IEnumerator<ChunkSlice> _slices;
    private readonly long _length;

    private Stream? _current;
    private long _position;
    private bool _finished;

    public ChunkRangeStream(IObjectStore store, string videoId, long chunkSize, long totalLength, ByteRange range)
    {
        _store = store;
        _videoId = videoId;
        _slices = ChunkMap.Slice(range, chunkSize, totalLength).GetEnumerator();
        _length = Math.Max(0, Math.Min(range.To, totalLength - 1) - range.From + 1);
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => _length;

    public override long Position
    {
        get => _position;
        set => throw new NotSupportedException("ChunkRangeStream is forward-only.");
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (buffer.IsEmpty || _finished) return 0;

        while (true)
        {
            if (_current is null)
            {
                if (!_slices.MoveNext())
                {
                    _finished = true;
                    return 0;
                }

                var slice = _slices.Current;
                var chunkKey = ChunkMap.ChunkKey(_videoId, slice.Index);
                _current = await _store
                    .OpenReadAsync(chunkKey, slice.OffsetInChunk, slice.Length, cancellationToken)
                    .ConfigureAwait(false);
            }

            var read = await _current.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            if (read > 0)
            {
                _position += read;
                return read;
            }

            // Current chunk slice exhausted: release it and fall through to the next one.
            await _current.DisposeAsync().ConfigureAwait(false);
            _current = null;
        }
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override int Read(byte[] buffer, int offset, int count)
        => ReadAsync(buffer.AsMemory(offset, count), CancellationToken.None).AsTask().GetAwaiter().GetResult();

    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override async ValueTask DisposeAsync()
    {
        if (_current is not null)
        {
            await _current.DisposeAsync().ConfigureAwait(false);
            _current = null;
        }
        _slices.Dispose();
        await base.DisposeAsync().ConfigureAwait(false);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _current?.Dispose();
            _current = null;
            _slices.Dispose();
        }
        base.Dispose(disposing);
    }
}
