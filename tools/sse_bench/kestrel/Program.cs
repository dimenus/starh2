// Kestrel arm of the SSE/mixed benchmark.
//
// Same contract as tools/sse_bench/server.go: GET / is the 13-byte oneshot
// body, GET /sse is a long-lived event stream of `data: <unix-nanos>\n\n`
// at a ticker interval (sleep to a deadline, not for a duration). TLS +
// HTTP/2 ALPN, same testdata cert as the other arms. Not the Datastar SDK
// — the client subtracts the stamped nanos, and a patchElements payload
// would be a different measurement.
using System.Net;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;

int port = 8444;
int intervalMs = 100;
string certPath = "testdata/cert.pem";
string keyPath = "testdata/key.pem";
for (var i = 0; i < args.Length; i++)
{
    var take = (string name) =>
    {
        if (i + 1 >= args.Length) throw new ArgumentException($"missing value for {name}");
        return args[++i];
    };
    switch (args[i])
    {
        case "-port":
        case "--port":
            port = int.Parse(take(args[i]));
            break;
        case "-sse-interval-ms":
        case "--sse-interval-ms":
            intervalMs = int.Parse(take(args[i]));
            break;
        case "-cert":
        case "--cert":
            certPath = take(args[i]);
            break;
        case "-key":
        case "--key":
            keyPath = take(args[i]);
            break;
        default:
            throw new ArgumentException($"unknown flag {args[i]}");
    }
}

// CreateBuilder otherwise also binds the default http://localhost:5000.
Environment.SetEnvironmentVariable("ASPNETCORE_URLS", "");

var interval = TimeSpan.FromMilliseconds(intervalMs);
var cert = LoadPem(certPath, keyPath);

var builder = WebApplication.CreateBuilder(new WebApplicationOptions
{
    Args = Array.Empty<string>(),
    EnvironmentName = "Production",
});
builder.Logging.ClearProviders();
builder.WebHost.ConfigureKestrel(k =>
{
    // Default is 100; run.sh's 200-stream recipe would stall the extras.
    k.Limits.Http2.MaxStreamsPerConnection = 1024;
    k.Listen(IPAddress.Loopback, port, listen =>
    {
        listen.Protocols = HttpProtocols.Http2;
        listen.UseHttps(cert);
    });
});

var app = builder.Build();

app.MapGet("/", async ctx =>
{
    ctx.Response.ContentType = "text/plain";
    await ctx.Response.WriteAsync("Hello, World!");
});

app.MapGet("/sse", async ctx =>
{
    ctx.Response.ContentType = "text/event-stream";
    ctx.Response.Headers.CacheControl = "no-cache";
    ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();
    await ctx.Response.StartAsync(ctx.RequestAborted);

    using var ticker = new PeriodicTimer(interval);
    var ct = ctx.RequestAborted;
    while (await ticker.WaitForNextTickAsync(ct))
    {
        // Ticks are 100ns; this is the same Unix epoch Go's UnixNano uses.
        var unixNs = (DateTime.UtcNow.Ticks - DateTime.UnixEpoch.Ticks) * 100;
        await ctx.Response.WriteAsync($"data: {unixNs}\n\n", ct);
        await ctx.Response.Body.FlushAsync(ct);
    }
});

// width is the scheduler width this arm actually runs with, so the harness
// can check that every arm is pinned the same. DOTNET_PROCESSOR_COUNT sets it
// and sizes the thread pool, the GC heaps, and Kestrel's IO queues from it.
app.Lifetime.ApplicationStarted.Register(() =>
    Console.WriteLine($"{{\"ready\":true,\"port\":{port},\"width\":{Environment.ProcessorCount}}}"));
app.Run();

static X509Certificate2 LoadPem(string certPath, string keyPath)
{
    using var loaded = X509Certificate2.CreateFromPemFile(certPath, keyPath);
    // CreateFromPemFile's key is not usable for Kestrel TLS on macOS/Windows
    // until it is re-imported from PFX. EphemeralKeySet is Windows-only and
    // throws PlatformNotSupportedException here.
    return X509CertificateLoader.LoadPkcs12(
        loaded.Export(X509ContentType.Pfx),
        password: null);
}
