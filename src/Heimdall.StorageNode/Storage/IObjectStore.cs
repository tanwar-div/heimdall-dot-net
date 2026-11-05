namespace Heimdall.StorageNode.Storage;

/// <summary>
/// The slice of object storage Heimdall actually needs. Keeping it this narrow is what lets the
/// tests run against an in-memory store with no MinIO container.
/// </summary>
public interface IObjectStore
{
    /// <summary>Creates the node's bucket when missing. Called once at startup.</summary>
    Task EnsureBucketAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes an object. <paramref name="length"/> is required because S3 PUT needs a content
    /// length up front, which is exactly why ingest buffers one chunk at a time rather than the
    /// whole upload.
    /// </summary>
    Task PutAsync(string key, Stream content, long length, string contentType, CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens a read over part of an object. The returned stream is the storage response body, so
    /// callers copy from it directly to the HTTP response.
    /// </summary>
    Task<Stream> OpenReadAsync(string key, long offset, long length, CancellationToken cancellationToken = default);

    /// <summary>Reads a whole small object as text. Used for manifests only.</summary>
    Task<string?> GetTextAsync(string key, CancellationToken cancellationToken = default);

    Task<bool> ExistsAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Deletes every object under a prefix; the number removed is returned.</summary>
    Task<int> DeletePrefixAsync(string prefix, CancellationToken cancellationToken = default);

    /// <summary>Round-trips a cheap call to prove the backing store is reachable.</summary>
    Task<bool> ProbeAsync(CancellationToken cancellationToken = default);
}
