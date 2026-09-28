using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;
using Npgsql;
using Relay.Core;
using Relay.Infrastructure;
using Relay.Worker;

var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole();
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(60));
builder.Services.AddRelay(builder.Configuration, "Relay.Worker");
builder.Services.AddOptions<RelayOptions>()
    .Validate(o => !string.IsNullOrWhiteSpace(o.ServiceBusConnectionString) || !string.IsNullOrWhiteSpace(o.ServiceBusNamespace), "Worker requires a broker connection or namespace")
    .Validate(o => !string.IsNullOrWhiteSpace(o.BrokerSourceId) && o.BrokerSourceId.Length <= 128, "Broker source lifecycle ID is required")
    .Validate(o => o.ParsedRoles.Length > 0 && o.ParsedRoles.All(r => r is "dispatcher" or "consumer" or "dlq-capture" or "retention"), "Worker roles are invalid")
    .ValidateOnStart();
builder.Services.AddSingleton(sp => ServiceBusTransport.Client(sp.GetRequiredService<IOptions<RelayOptions>>().Value));
builder.Services.AddSingleton<IMessagePublisher, ServiceBusPublisher>();
builder.Services.AddKeyedSingleton<NpgsqlDataSource>("dispatcher", (sp, _) => Runtime.DataSource(sp.GetRequiredService<IOptions<RelayOptions>>().Value.Database, "Relay.Dispatcher"));
builder.Services.AddSingleton(sp => new Dispatcher(sp.GetRequiredKeyedService<NpgsqlDataSource>("dispatcher"),
    sp.GetRequiredService<IMessagePublisher>(), sp.GetRequiredService<IOptions<RelayOptions>>()));
builder.Services.AddSingleton<WorkerService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<WorkerService>());
var app = builder.Build();
app.MapGet("/health/live", () => Results.Ok(new { status = "alive" }));
app.MapGet("/health/ready", async (IRelayStore store, ServiceBusClient client, WorkerService worker, IOptions<RelayOptions> options, CancellationToken ct) =>
{
    using var budget = CancellationTokenSource.CreateLinkedTokenSource(ct);
    budget.CancelAfter(TimeSpan.FromSeconds(3));
    try
    {
        if (!await store.ProbeAsync(budget.Token) || worker.HasConsumer && !worker.ConsumerRunning) return Results.StatusCode(503);
        await ServiceBusTransport.ProbeAsync(client, options.Value, budget.Token);
        return Results.Ok(new { status = "ready" });
    }
    catch (Exception) when (!ct.IsCancellationRequested) { return Results.StatusCode(503); }
});
await app.RunAsync();
