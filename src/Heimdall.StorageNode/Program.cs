using Heimdall.Shared.Cluster;
using Heimdall.Shared.Http;
using Heimdall.Shared.Security;
using Heimdall.StorageNode;
using Heimdall.StorageNode.Endpoints;
using Heimdall.StorageNode.Replication;
using Heimdall.StorageNode.Storage;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

// ---- configuration ---------------------------------------------------------------------------

// One cluster manifest, mounted into every container. Gateway and storage nodes read the identical
// node list and each builds its own ring from it - that shared input is what replaces a
// coordinator or a lookup service.
builder.Configuration.AddJsonFile("cluster.json", optional: true, reloadOnChange: false);

builder.Services.AddOptions<NodeOptions>()
    .Bind(builder.Configuration.GetSection(NodeOptions.SectionName))
    .Validate(o => { o.Validate(); return true; }, "Invalid Node configuration.")
    .ValidateOnStart();

builder.Services.AddOptions<StorageOptions>()
    .Bind(builder.Configuration.GetSection(StorageOptions.SectionName))
    .Validate(o => { o.Validate(); return true; }, "Invalid Storage configuration.")
    .ValidateOnStart();

builder.Services.AddOptions<ClusterOptions>()
    .Bind(builder.Configuration.GetSection(ClusterOptions.SectionName))
    .Validate(o => { o.Validate(); return true; }, "Invalid Cluster configuration.")
    .ValidateOnStart();

// ---- kestrel ---------------------------------------------------------------------------------

builder.WebHost.ConfigureKestrel(options =>
{
    // Uploads are streamed straight into object storage, so there is no reason to cap them at the
    // default 30 MB. The practical limit is whatever the object store will hold.
    options.Limits.MaxRequestBodySize = null;
    options.Limits.MinRequestBodyDataRate = null;     // a slow uploader on a big file is not an attack
    options.Limits.MinResponseDataRate = null;        // nor is a paused video player
    options.AllowSynchronousIO = false;
});

// ---- services --------------------------------------------------------------------------------

builder.Services.AddSingleton<ClusterTopology>();
builder.Services.AddSingleton<IObjectStore, S3ObjectStore>();
builder.Services.AddSingleton<ChunkedVideoRepository>();
builder.Services.AddSingleton<ReplicationQueue>();
builder.Services.AddHostedService<ReplicationWorker>();

builder.Services.AddSingleton(sp =>
    new ApiKeyGuard(sp.GetRequiredService<IOptions<ClusterOptions>>().Value.ApiKey));

builder.Services.AddHttpClient(StorageNodeClient.HttpClientName, http =>
{
    // Replicating a multi-gigabyte video can take minutes. The retry policy, not HttpClient's
    // timeout, is what bounds a replication attempt.
    http.Timeout = Timeout.InfiniteTimeSpan;
})
.ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
{
    ConnectTimeout = TimeSpan.FromSeconds(5),
    PooledConnectionLifetime = TimeSpan.FromMinutes(5),
    AllowAutoRedirect = false,
    MaxConnectionsPerServer = 64,
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

builder.Services.AddProblemDetails();

var app = builder.Build();

// ---- startup checks --------------------------------------------------------------------------

var nodeOptions = app.Services.GetRequiredService<IOptions<NodeOptions>>().Value;
var topology = app.Services.GetRequiredService<ClusterTopology>();
var startupLogger = app.Services.GetRequiredService<ILogger<Program>>();

if (!topology.Ring.TryGetNode(nodeOptions.Id, out _))
{
    // The node id is what gets hashed onto the ring. If it is not in Cluster:Nodes this process can
    // never be anyone's primary, which is a configuration bug worth failing loudly on.
    throw new InvalidOperationException(
        $"Node:Id '{nodeOptions.Id}' is not present in Cluster:Nodes " +
        $"({string.Join(", ", topology.Ring.Nodes.Select(n => n.Id))}). " +
        "The ring hashes node ids, so this node would own no keyspace.");
}

await app.Services.GetRequiredService<IObjectStore>().EnsureBucketAsync();

startupLogger.LogInformation(
    "Heimdall storage node {NodeId} ready. Ring: {NodeCount} nodes / {TokenCount} tokens, RF={Factor}, chunk={ChunkSize} bytes.",
    nodeOptions.Id,
    topology.Ring.NodeCount,
    topology.Ring.TokenCount,
    topology.ReplicationFactor,
    nodeOptions.ChunkSizeBytes);

app.UseExceptionHandler();
app.UseStatusCodePages();
app.MapVideoEndpoints();

await app.RunAsync();
