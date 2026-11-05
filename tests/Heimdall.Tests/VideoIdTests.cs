using Heimdall.Gateway.Routing;
using Heimdall.Shared.Cluster;

namespace Heimdall.Tests;

public class VideoIdTests
{
    [Theory]
    [InlineData("Interstellar (2014).mp4", "interstellar-2014")]
    [InlineData("my holiday video.MOV", "my-holiday-video")]
    [InlineData("C:\\Users\\div\\clip.mp4", "clip")]
    [InlineData("/home/div/videos/clip.mkv", "clip")]
    [InlineData("....mp4", "")]
    [InlineData("", "")]
    [InlineData(null, "")]
    [InlineData("___", "")]
    [InlineData("a.b.c.mp4", "a-b-c")]
    public void Slugify_reduces_a_file_name_to_url_safe_text(string? fileName, string expected)
        => Assert.Equal(expected, VideoId.Slugify(fileName));

    [Fact]
    public void Generated_ids_are_valid_and_unique()
    {
        var ids = Enumerable.Range(0, 500).Select(_ => VideoId.Create("holiday video.mp4")).ToArray();

        Assert.All(ids, id =>
        {
            Assert.True(VideoId.IsValid(id), $"'{id}' failed validation.");
            Assert.StartsWith("holiday-video-", id);
        });

        Assert.Equal(ids.Length, ids.Distinct().Count());
    }

    [Fact]
    public void A_nameless_upload_still_gets_a_usable_id()
    {
        var id = VideoId.Create(null);

        Assert.StartsWith("video-", id);
        Assert.True(VideoId.IsValid(id));
    }

    [Fact]
    public void A_very_long_file_name_is_truncated_to_the_id_length_limit()
    {
        var id = VideoId.Create(new string('a', 400) + ".mp4");

        Assert.True(id.Length <= VideoId.MaxLength);
        Assert.True(VideoId.IsValid(id));
    }

    [Theory]
    [InlineData("clip", true)]
    [InlineData("clip-123_v2.final", true)]
    [InlineData("", false)]
    [InlineData(".", false)]
    [InlineData("..", false)]
    [InlineData("../../etc/passwd", false)]     // ids become object-key prefixes
    [InlineData("clip/chunk-00000", false)]
    [InlineData("clip with spaces", false)]
    [InlineData("clip\u00e9", false)]
    public void Validation_rejects_anything_that_could_escape_an_object_key_prefix(string id, bool expected)
        => Assert.Equal(expected, VideoId.IsValid(id));

    [Fact]
    public void Two_uploads_of_the_same_file_name_land_on_different_ring_positions()
    {
        // The random suffix is not just for uniqueness: it is what keeps a popular file name from
        // piling every copy onto one node.
        var ring = new ConsistentHashRing(TestCluster.Nodes(6));

        var owners = Enumerable.Range(0, 200)
            .Select(_ => ring.GetPrimary(VideoId.Create("trailer.mp4")).Id)
            .Distinct()
            .ToArray();

        Assert.Equal(6, owners.Length);
    }
}
