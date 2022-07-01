// See https://aka.ms/new-console-template for more information

using DistributedRateLimiting.Orleans;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Hosting;
using System.Net;
using System.Threading.RateLimiting;

var hostBuilder = Host.CreateDefaultBuilder(args);
hostBuilder
    .UseOrleans(silo =>
    {
        int instanceId = 0;
        if (args is { Length: > 0 })
        {
            instanceId = int.Parse(args[0]);
        }

        silo.UseLocalhostClustering(
            siloPort: 11111 + instanceId,
            gatewayPort: 30000 + instanceId,
            primarySiloEndpoint: new IPEndPoint(IPAddress.Loopback, 11111));
    })
    .ConfigureServices(services =>
    {
        services.AddDistributedRateLimiter(options =>
        {
            options.QueueLimit = 200;
            options.GlobalPermitCount = 100;
            options.TargetPermitsPerClient = 20;
        });
    })
    .UseConsoleLifetime();
var host = await hostBuilder.StartAsync();

var rateLimiter = host.Services.GetRequiredService<RateLimiter>();
long numLeaseHolders = 0;
var cancellationToken = new CancellationTokenSource();
var tasks = new List<Task>();
for (var i = 0; i < 5; i++)
{
    var name = $"worker-{i}";
    tasks.Add(Task.Run(() => RunWorker(rateLimiter, name, cancellationToken.Token)));
}

Console.CancelKeyPress += (_, __) => cancellationToken.Cancel();
await Task.WhenAll(tasks);
await host.StopAsync();

host.Dispose();

async Task RunWorker(RateLimiter rateLimiter, string name, CancellationToken cancellationToken)
{
    const int PermitCount = 25;
    while (!cancellationToken.IsCancellationRequested)
    {
        RateLimitLease lease;
        do
        {
            lease = rateLimiter.Acquire(PermitCount);
            if (lease.IsAcquired) break;
            Console.WriteLine($"{name}: Waiting for lease (holders: {numLeaseHolders})");
            lease = await rateLimiter.WaitAsync(PermitCount);
        } while (!lease.IsAcquired);

        var holders = Interlocked.Increment(ref numLeaseHolders);
        Console.WriteLine($"{name}: Acquired lease (holders: {holders})");
        await Task.Delay(500);
        lease.Dispose();
        holders = Interlocked.Decrement(ref numLeaseHolders);
        Console.WriteLine($"{name}: Disposed lease (holders: {holders})");
        await Task.Delay(500);
    }
}
