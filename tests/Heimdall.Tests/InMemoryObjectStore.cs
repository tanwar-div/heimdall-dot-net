using System.Collections.Concurrent;
using System.Text;
using Heimdall.StorageNode.Storage;

namespace Heimdall.Tests;

/// <summary>
/// <see cref="IObjectStore"/> backed by a dictionary. Lets the chunking and range logic be tested
/// for byte-exactness without a MinIO container in the loop.
/// </summary>
internal sealed class InMemoryObjectStore : IObjectStore
{
    private readonly ConcurrentDictionary<string, byte[]> _objects = new(StringComparer.Ordinal);

    public int PutCount { get; private set; }
    public int RangeReadCount { get; private set; }

    /// <summary>Keys currently stored, sorted - mirrors what a bucket listing would show.</summary>
    public IReadOnlyList<string> Keys => _objects.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray();

    public byte[]? this[string key] => _objects.TryGetValue(key, out var value) ? value : null;

    public Task EnsureBucketAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public async Task PutAsync(string key, Stream content, long length, string contentType, CancellationToken cancellationToken = default)
    {
        using var buffer = new MemoryStream();
        await content.CopyToAsync(buffer, cancellationToken);
        var bytes = buffer.ToArray();

        Assert.Equal(length, bytes.Length);     // the store is strict about the declared length
        _objects[key] = bytes;
        PutCount++;
    }

    public Task<Stream> OpenReadAsync(string key, long offset, long length, CancellationToken cancellationToken = default)
    {
        RangeReadCount++;

        if (!_objects.TryGetValue(key, out var bytes))
            throw new KeyNotFoundException($"No object '{key}'.");

        if (offset < 0 || offset + length > bytes.Length)
            throw new ArgumentOutOfRangeException(nameof(offset), $"Range {offset}+{length} is outside '{key}' ({bytes.Length} bytes).");

        return Task.FromResult<Stream>(new MemoryStream(bytes, (int)offset, (int)length, writable: false));
    }

    public Task<string?> GetTextAsync(string key, CancellationToken cancellationToken = default)
        => Task.FromResult(_objects.TryGetValue(key, out var bytes) ? Encoding.UTF8.GetString(bytes) : null);

    public Task<bool> ExistsAsync(string key, CancellationToken cancellationToken = default)
        => Task.FromResult(_objects.ContainsKey(key));

    public Task<int> DeletePrefixAsync(string prefix, CancellationToken cancellationToken = default)
    {
        var matched = _objects.Keys.Where(k => k.StartsWith(prefix, StringComparison.Ordinal)).ToArray();
        foreach (var key in matched) _objects.TryRemove(key, out _);
        return Task.FromResult(matched.Length);
    }

    public Task<bool> ProbeAsync(CancellationToken cancellationToken = default) => Task.FromResult(true);
}

/// <summary>
/// A stream that hands back at most <c>maxPerRead</c> bytes per call, which is what a network
/// stream does. Chunk boundaries have to be exact regardless of how the source fragments, so the
/// ingest path is tested against this as well as against a <see cref="MemoryStream"/>.
/// </summary>
internal sealed class DripStream(byte[] data, int maxPerRead) : Stream
{
    private int _position;

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => data.Length;

    public override long Position
    {
        get => _position;
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        var take = Math.Min(Math.Min(count, maxPerRead), data.Length - _position);
        if (take <= 0) return 0;

        Array.Copy(data, _position, buffer, offset, take);
        _position += take;
        return take;
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        var take = Math.Min(Math.Min(buffer.Length, maxPerRead), data.Length - _position);
        if (take <= 0) return ValueTask.FromResult(0);

        data.AsSpan(_position, take).CopyTo(buffer.Span);
        _position += take;
        return ValueTask.FromResult(take);
    }

    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
