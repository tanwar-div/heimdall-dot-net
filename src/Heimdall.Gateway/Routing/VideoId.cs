using System.Text;

namespace Heimdall.Gateway.Routing;

/// <summary>
/// Builds video ids. The id is the ring key, so it has to be the thing clients use to fetch the
/// video; deriving it from the file name keeps URLs readable while the random suffix keeps two
/// uploads of <c>movie.mp4</c> from colliding (and, incidentally, keeps them on different nodes).
/// </summary>
public static class VideoId
{
    public const int MaxLength = 128;

    public static string Create(string? fileName)
    {
        var slug = Slugify(fileName);
        var suffix = Guid.NewGuid().ToString("n")[..12];

        if (slug.Length == 0) return $"video-{suffix}";

        // Leave room for the suffix and separator.
        var budget = MaxLength - suffix.Length - 1;
        if (slug.Length > budget) slug = slug[..budget];

        return $"{slug}-{suffix}";
    }

    internal static string Slugify(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return string.Empty;

        // Strip any directory component a browser may have included in a multipart filename.
        var name = value.Replace('\\', '/');
        var lastSlash = name.LastIndexOf('/');
        if (lastSlash >= 0) name = name[(lastSlash + 1)..];

        var dot = name.LastIndexOf('.');
        if (dot > 0) name = name[..dot];

        var builder = new StringBuilder(name.Length);
        var lastWasDash = false;

        foreach (var c in name)
        {
            if (char.IsAsciiLetterOrDigit(c))
            {
                builder.Append(char.ToLowerInvariant(c));
                lastWasDash = false;
            }
            else if (!lastWasDash && builder.Length > 0)
            {
                builder.Append('-');
                lastWasDash = true;
            }
        }

        return builder.ToString().Trim('-');
    }

    /// <summary>Same validation the storage node applies, since the id becomes an object key prefix.</summary>
    public static bool IsValid(string videoId)
    {
        if (string.IsNullOrEmpty(videoId) || videoId.Length > MaxLength) return false;
        if (videoId is "." or "..") return false;

        foreach (var c in videoId)
        {
            if (!char.IsAsciiLetterOrDigit(c) && c is not ('-' or '_' or '.')) return false;
        }

        return true;
    }
}
