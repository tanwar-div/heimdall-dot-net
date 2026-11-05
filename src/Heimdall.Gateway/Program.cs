using Heimdall.Gateway;
using Heimdall.Gateway.Endpoints;
using Heimdall.Gateway.Proxy;
using Heimdall.Gateway.Routing;
using Heimdall.Shared.Cluster;
using Heimdall.Shared.Http;
using Heimdall.Shared.Security;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

// ---- configuration ---------------------------------------------------------------------------

// One cluster manifest, mounted into every container. Gateway and storage nodes read the identical
// node list and each builds its own ring from it - that shared input is what replaces a
// coordinator or a lookup service.
builder.Configuration.AddJsonFile("cluster.json", optional: true, reloadOnChange: false);

builder.Services.AddOptions<ClusterOptions>()
    .Bind(builder.Configuration.GetSection(ClusterOptions.SectionName))
    .Validate(o => { o.Validate(); return true; }, "Invalid Cluster configuration.")
    .ValidateOnStart();

builder.Services.AddOptions<GatewayOptions>()
    .Bind(builder.Configuration.GetSection(GatewayOptions.SectionName))
    .Validate(o => { o.Validate(); return true; }, "Invalid Gateway configuration.")
    .ValidateOnStart();

// ---- kestrel ---------------------------------------------------------------------------------

builder.WebHost.ConfigureKestrel(options =>
{
    // Uploads pass through to a storage node without being buffered, so the gateway imposes no
    // size ceiling of its own.
    options.Limits.MaxRequestBodySize = null;
    options.Limits.MinRequestBodyDataRate = null;
    options.Limits.MinResponseDataRate = null;
    options.AllowSynchronousIO = false;
});

// ---- services --------------------------------------------------------------------------------

builder.Services.AddSingleton<ClusterTopology>();
builder.Services.AddSingleton<VideoStreamProxy>();
builder.Services.AddSingleton<VideoUploadService>();

builder.Services.AddSingleton(sp =>
    new ApiKeyGuard(sp.GetRequiredService<IOptions<ClusterOptions>>().Value.ApiKey));

builder.Services.AddHttpClient(StorageNodeClient.HttpClientName, http =>
{
    // No client-side timeout: a seeking player may hold a range request open for a long time, and
    // the gateway bounds only the wait for *headers* (Gateway:CandidateHeaderTimeout).
    http.Timeout = Timeout.InfiniteTimeSpan;
})
.ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
{
    // A short connect timeout is what makes failover to the next replica feel instant rather than
    // waiting out the OS-level TCP backoff.
    ConnectTimeout = TimeSpan.FromSeconds(2),
    PooledConnectionLifetime = TimeSpan.FromMinutes(5),
    PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
    MaxConnectionsPerServer = 256,
    AllowAutoRedirect = false,
});

builder.Services.AddSingleton(sp =>
{
    var factory = sp.GetRequiredService<IHttpClientFactory>();
    var cluster = sp.GetRequiredService<IOptions<ClusterOptions>>().Value;
    return new StorageNodeClient(
        factory.CreateClient(StorageNodeClient.HttpClientName),
        sp.GetRequiredService<ILogger<StorageNodeClient>>(),
        cluster.ApiKey);
});

builder.Services.AddCors(options => options.AddDefaultPolicy(policy => policy
    .AllowAnyOrigin()
    .AllowAnyHeader()
    .AllowAnyMethod()
    // Range requests from a cross-origin <video> element are useless unless the browser can read
    // the partial-content headers back.
    .WithExposedHeaders("Content-Range", "Accept-Ranges", "Content-Length", "ETag",
        "X-Heimdall-Served-By", "X-Heimdall-Primary-Node", "X-Heimdall-Failover-From",
        "X-Heimdall-Read-Candidates")));

builder.Services.AddProblemDetails();

var app = builder.Build();

var topology = app.Services.GetRequiredService<ClusterTopology>();
app.Services.GetRequiredService<ILogger<Program>>().LogInformation(
    "Heimdall gateway ready. Ring: {NodeCount} nodes / {TokenCount} tokens, RF={Factor}, {ReadPct}% of replicas selected for reads.",
    topology.Ring.NodeCount,
    topology.Ring.TokenCount,
    topology.ReplicationFactor,
    topology.LoadBalancer.ReadReplicaPercentage);

app.UseExceptionHandler();
app.UseStatusCodePages();
app.UseCors();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapGet("/health", () => Results.Ok(new { status = "healthy", service = "heimdall-gateway" }));
app.MapVideoEndpoints();
app.MapClusterEndpoints();

await app.RunAsync();
