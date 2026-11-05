namespace Heimdall.Shared.Http;

/// <summary>
/// The storage node's internal API surface, in one place so the gateway client and the node's own
/// endpoint registrations cannot drift apart.
/// </summary>
public static class StorageNodeRoutes
{
    public const string Health = "health";
    public const string Ready = "ready";
    public const string NodeInfo = "internal/node";

    public static string Video(string videoId) => $"internal/videos/{Uri.EscapeDataString(videoId)}";
    public static string Manifest(string videoId) => $"internal/videos/{Uri.EscapeDataString(videoId)}/manifest";

    public const string VideoTemplate = "/internal/videos/{videoId}";
    public const string ManifestTemplate = "/internal/videos/{videoId}/manifest";
}
