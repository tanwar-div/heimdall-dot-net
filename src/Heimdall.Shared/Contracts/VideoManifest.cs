using System.Text.Json.Serialization;

namespace Heimdall.Shared.Contracts;

/// <summary>
/// The per-video metadata object a storage node writes alongside the chunks. It is the only thing
/// that has to be read before serving a range request, and it is small enough to be a single
/// object-storage GET.
/// </summary>
public sealed record VideoManifest
{
    public required string VideoId { get; init; }

    /// <summary>Total length in bytes of the original upload.</summary>
    public required long Length { get; init; }

    /// <summary>Chunk size this video was stored with. Read from the manifest, never assumed.</summary>
    public required long ChunkSize { get; init; }

    public required int ChunkCount { get; init; }

    public string ContentType { get; init; } = "application/octet-stream";

    public string? FileName { get; init; }

    /// <summary>Hex SHA-256 of the full byte stream, computed while ingesting. Used as the HTTP ETag.</summary>
    public required string ContentHash { get; init; }

    /// <summary>Node that ingested the upload, i.e. the ring's primary for this id.</summary>
    public required string OwnerNodeId { get; init; }

    /// <summary>Node this copy physically lives on. Differs from <see cref="OwnerNodeId"/> on replicas.</summary>
    public required string StoredOnNodeId { get; init; }

    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>True when this copy arrived through replication rather than a client upload.</summary>
    public bool IsReplica { get; init; }

    [JsonIgnore]
    public string ETag => $"\"{ContentHash}\"";
}

/// <summary>Response body for a completed upload.</summary>
public sealed record UploadResult
{
    public required string VideoId { get; init; }
    public required long Length { get; init; }
    public required string ContentHash { get; init; }
    public required string PrimaryNodeId { get; init; }
    public required string[] ReplicaNodeIds { get; init; }
    public required string StreamUrl { get; init; }
}

/// <summary>Per-node replication state, reported by a primary for diagnostics.</summary>
public sealed record ReplicationStatus
{
    public required string VideoId { get; init; }
    public required string TargetNodeId { get; init; }
    public required string State { get; init; }
    public int Attempts { get; init; }
    public string? Error { get; init; }
    public DateTimeOffset UpdatedAt { get; init; } = DateTimeOffset.UtcNow;
}
