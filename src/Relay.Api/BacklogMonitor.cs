using Relay.Core;
using Relay.Infrastructure;

namespace Relay.Api;

public sealed class BacklogMonitor(IRelayStore store, ILogger<BacklogMonitor> logger) : BackgroundService
{
    public sealed record Snapshot(Backlog Backlog, DateTimeOffset At);
    private Snapshot _snapshot = new(new(0, 0), DateTimeOffset.MinValue);
    public Snapshot Current => Volatile.Read(ref _snapshot);
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Telemetry.Meter.CreateObservableGauge("relay_outbox_pending", () => Current.Backlog.Count);
        Telemetry.Meter.CreateObservableGauge("relay_outbox_oldest_age_seconds", () => Current.Backlog.OldestSeconds);
        while (!stoppingToken.IsCancellationRequested)
        {
            try { Volatile.Write(ref _snapshot, new(await store.BacklogAsync(stoppingToken), DateTimeOffset.UtcNow)); }
            catch (Exception e) when (!stoppingToken.IsCancellationRequested) { logger.LogWarning("Backlog snapshot delayed; category {Category}", e.GetType().Name); }
            await Task.Delay(1000, stoppingToken);
        }
    }
}
