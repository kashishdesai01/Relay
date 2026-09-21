using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Relay.Core;

namespace Relay.Infrastructure;

public sealed class RelayOptions
{
    public string Database { get; set; } = "";
    public string ServiceBusConnectionString { get; set; } = "";
    public string ServiceBusNamespace { get; set; } = "";
    public string Queue { get; set; } = "relay";
    public string Roles { get; set; } = "dispatcher,consumer,dlq-capture,retention";
    public string BrokerSourceId { get; set; } = "local-v1";
    public string[] ParsedRoles => Roles.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
    public bool ReadinessUsesReceiver => ParsedRoles.Any(role => role is "consumer" or "dlq-capture");
    public int BatchSize { get; set; } = 16;
    public long BacklogLimit { get; set; } = 10000;
    public int OldestPendingLimitSeconds { get; set; } = 60;
    public int ProducerRequestsPerSecond { get; set; } = 100;
}

public static class Telemetry
{
    public const string Name = "Relay";
    public static readonly ActivitySource Activities = new(Name);
    public static readonly Meter Meter = new(Name);
    public static readonly Counter<long> Accepted = Meter.CreateCounter<long>("relay_events_accepted_total");
    public static readonly Counter<long> Rejected = Meter.CreateCounter<long>("relay_events_rejected_total");
    public static readonly Counter<long> Consumed = Meter.CreateCounter<long>("relay_consume_total");
    public static readonly Counter<long> DispatchFailures = Meter.CreateCounter<long>("relay_dispatch_failures_total");
    public static readonly Histogram<double> IngestDuration = Meter.CreateHistogram<double>("relay_ingest_duration_seconds", "s");
    public static readonly Histogram<double> ConsumeDuration = Meter.CreateHistogram<double>("relay_consume_duration_seconds", "s");
    public static readonly Histogram<double> EndToEnd = Meter.CreateHistogram<double>("relay_end_to_end_latency_seconds", "s");
    public static ActivityContext Parent(string? parent) => ActivityContext.TryParse(parent, null, true, out var context) ? context : default;
}

public static class Runtime
{
    public static IServiceCollection AddRelay(this IServiceCollection services, IConfiguration configuration, string serviceName)
    {
        AppContext.SetSwitch("Azure.Experimental.EnableActivitySource", true);
        services.AddOptions<RelayOptions>().Bind(configuration.GetSection("Relay"))
            .Validate(o => !string.IsNullOrWhiteSpace(o.Database), "Relay:Database is required")
            .Validate(o => o.BatchSize is > 0 and <= 100 && o.BacklogLimit > 0 && o.OldestPendingLimitSeconds > 0 && o.ProducerRequestsPerSecond > 0, "Limits must be positive")
            .ValidateOnStart();
        services.AddSingleton(TimeProvider.System);
        services.AddSingleton(sp => DataSource(sp.GetRequiredService<IOptions<RelayOptions>>().Value.Database, serviceName));
        services.AddSingleton<IRelayStore, PostgresStore>();
        services.AddOpenTelemetry().ConfigureResource(r => r.AddService(serviceName))
            .WithTracing(t => t.AddSource(Telemetry.Name).AddSource("Azure.Messaging.ServiceBus", "Azure.Messaging.ServiceBus.*")
                .AddAspNetCoreInstrumentation().AddOtlpExporter())
            .WithMetrics(m => m.AddMeter(Telemetry.Name).AddAspNetCoreInstrumentation().AddOtlpExporter());
        return services;
    }
    public static NpgsqlDataSource DataSource(string connectionString, string applicationName)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString)
        { ApplicationName = applicationName, MaxPoolSize = 24, Timeout = 5, CommandTimeout = 10, IncludeErrorDetail = false };
        return NpgsqlDataSource.Create(builder.ConnectionString);
    }
}
