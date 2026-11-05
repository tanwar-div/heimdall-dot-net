using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Options;

namespace Heimdall.StorageNode.Storage;

/// <summary>
/// <see cref="IObjectStore"/> over any S3-compatible endpoint. In this cluster that endpoint is
/// MinIO, addressed path-style with a bucket per storage node.
/// </summary>
public sealed class S3ObjectStore : IObjectStore, IDisposable
{
    private readonly IAmazonS3 _s3;
    private readonly StorageOptions _options;
    private readonly ILogger<S3ObjectStore> _logger;

    public S3ObjectStore(IOptions<StorageOptions> options, ILogger<S3ObjectStore> logger)
    {
        _options = options.Value;
        _options.Validate();
        _logger = logger;

        var config = new AmazonS3Config
        {
            ServiceURL = _options.Endpoint,
            ForcePathStyle = _options.ForcePathStyle,
            AuthenticationRegion = _options.Region,
            // MinIO over plain HTTP inside the compose network; TLS is terminated at the edge.
            UseHttp = _options.Endpoint.StartsWith("http://", StringComparison.OrdinalIgnoreCase),
        };

        _s3 = new AmazonS3Client(_options.AccessKey, _options.SecretKey, config);
    }

    public string Bucket => _options.Bucket;

    public async Task EnsureBucketAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await _s3.GetBucketLocationAsync(new GetBucketLocationRequest { BucketName = Bucket }, cancellationToken)
                .ConfigureAwait(false);
            return;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode is System.Net.HttpStatusCode.NotFound
                                           || ex.ErrorCode is "NoSuchBucket")
        {
            if (!_options.CreateBucketIfMissing)
                throw new InvalidOperationException($"Bucket '{Bucket}' does not exist and Storage:CreateBucketIfMissing is false.");
        }

        _logger.LogInformation("Creating bucket {Bucket} on {Endpoint}.", Bucket, _options.Endpoint);
        try
        {
            await _s3.PutBucketAsync(new PutBucketRequest { BucketName = Bucket, UseClientRegion = false }, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (AmazonS3Exception ex) when (ex.ErrorCode is "BucketAlreadyOwnedByYou" or "BucketAlreadyExists")
        {
            // Two nodes sharing a bucket name raced us to it. Harmless.
        }
    }

    public async Task PutAsync(
        string key,
        Stream content,
        long length,
        string contentType,
        CancellationToken cancellationToken = default)
    {
        var request = new PutObjectRequest
        {
            BucketName = Bucket,
            Key = key,
            InputStream = content,
            ContentType = contentType,
            // The caller owns the buffer this stream wraps and reuses it for the next chunk.
            AutoCloseStream = false,
            AutoResetStreamPosition = true,
            // aws-chunked framing adds nothing here: each chunk is already a bounded, seekable
            // body, and MinIO is happy with a plain signed PUT.
            UseChunkEncoding = false,
        };
        request.Headers.ContentLength = length;

        await _s3.PutObjectAsync(request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<Stream> OpenReadAsync(
        string key,
        long offset,
        long length,
        CancellationToken cancellationToken = default)
    {
        if (length <= 0) return Stream.Null;

        var request = new GetObjectRequest
        {
            BucketName = Bucket,
            Key = key,
            ByteRange = new ByteRange(offset, offset + length - 1),
        };

        // The response owns the network stream; wrapping it keeps the response alive exactly as
        // long as the stream the caller is copying from.
        var response = await _s3.GetObjectAsync(request, cancellationToken).ConfigureAwait(false);
        return new ResponseBoundStream(response);
    }

    public async Task<string?> GetTextAsync(string key, CancellationToken cancellationToken = default)
    {
        try
        {
            using var response = await _s3.GetObjectAsync(
                new GetObjectRequest { BucketName = Bucket, Key = key },
                cancellationToken).ConfigureAwait(false);

            using var reader = new StreamReader(response.ResponseStream);
            return await reader.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<bool> ExistsAsync(string key, CancellationToken cancellationToken = default)
    {
        try
        {
            await _s3.GetObjectMetadataAsync(
                new GetObjectMetadataRequest { BucketName = Bucket, Key = key },
                cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    public async Task<int> DeletePrefixAsync(string prefix, CancellationToken cancellationToken = default)
    {
        var deleted = 0;
        string? continuationToken = null;

        do
        {
            var listed = await _s3.ListObjectsV2Async(
                new ListObjectsV2Request
                {
                    BucketName = Bucket,
                    Prefix = prefix,
                    ContinuationToken = continuationToken,
                },
                cancellationToken).ConfigureAwait(false);

            if (listed.S3Objects.Count > 0)
            {
                await _s3.DeleteObjectsAsync(
                    new DeleteObjectsRequest
                    {
                        BucketName = Bucket,
                        Objects = listed.S3Objects.Select(o => new KeyVersion { Key = o.Key }).ToList(),
                    },
                    cancellationToken).ConfigureAwait(false);

                deleted += listed.S3Objects.Count;
            }

            continuationToken = listed.IsTruncated == true ? listed.NextContinuationToken : null;
        }
        while (continuationToken is not null);

        return deleted;
    }

    public async Task<bool> ProbeAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await _s3.ListObjectsV2Async(
                new ListObjectsV2Request { BucketName = Bucket, MaxKeys = 1 },
                cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Object store probe failed for bucket {Bucket}.", Bucket);
            return false;
        }
    }

    public void Dispose() => _s3.Dispose();

    /// <summary>
    /// Read-only stream that keeps its <see cref="GetObjectResponse"/> alive. Without this the
    /// response would be disposed while the caller is still copying bytes out of it.
    /// </summary>
    private sealed class ResponseBoundStream : Stream
    {
        private readonly GetObjectResponse _response;
        private readonly Stream _inner;

        public ResponseBoundStream(GetObjectResponse response)
        {
            _response = response;
            _inner = response.ResponseStream;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _response.ContentLength;

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);

        public override int Read(Span<byte> buffer) => _inner.Read(buffer);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => _inner.ReadAsync(buffer, cancellationToken);

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => _inner.ReadAsync(buffer, offset, count, cancellationToken);

        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _inner.Dispose();
                _response.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}
