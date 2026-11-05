using Heimdall.Shared.Contracts;
using Heimdall.Shared.Security;
using Microsoft.AspNetCore.Http;

namespace Heimdall.Tests;

public class ApiKeyGuardTests
{
    private static HttpRequest RequestWith(string? key)
    {
        var context = new DefaultHttpContext();
        if (key is not null) context.Request.Headers[HeimdallHeaders.ApiKey] = key;
        return context.Request;
    }

    [Fact]
    public void The_configured_key_is_accepted()
        => Assert.True(new ApiKeyGuard("s3cret").IsAuthorized(RequestWith("s3cret")));

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("wrong")]
    [InlineData("s3cre")]        // prefix
    [InlineData("s3cretx")]      // extension
    [InlineData("S3CRET")]       // case matters
    public void Anything_else_is_rejected(string? provided)
        => Assert.False(new ApiKeyGuard("s3cret").IsAuthorized(RequestWith(provided)));

    [Fact]
    public void With_no_key_configured_writes_are_disabled_rather_than_open()
    {
        // Failing closed matters: an unset key must not mean "no authentication required".
        var guard = new ApiKeyGuard(null);

        Assert.False(guard.IsConfigured);
        Assert.False(guard.IsAuthorized(RequestWith("anything")));
        Assert.False(guard.TryAuthorize(RequestWith("anything"), out _));
    }

    [Fact]
    public void TryAuthorize_passes_a_valid_key_and_blocks_an_invalid_one()
    {
        var guard = new ApiKeyGuard("s3cret");

        Assert.True(guard.TryAuthorize(RequestWith("s3cret"), out _));
        Assert.False(guard.TryAuthorize(RequestWith("nope"), out var failure));
        Assert.NotNull(failure);
    }
}
