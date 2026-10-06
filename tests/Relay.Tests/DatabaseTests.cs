using Microsoft.Extensions.Options;
using Relay.Contracts;
using Relay.Core;
using Relay.Infrastructure;
using Xunit;

namespace Relay.Tests;

[Collection("Database")]
public sealed class DatabaseTests(PostgresFixture fixture)
{
    [Fact, Trait("Category", "Integration")]
    public async Task ForeignIncidentPreservesBoundedDiagnosticsForAuthenticatedInspection()
    {
        await fixture.Reset();
        var raw = System.Text.Encoding.UTF8.GetBytes("not-json");
        await fixture.Store.CaptureAsync("source/queue", "foreign:42", null, "invalid_transport", raw, default,
            new("unexpected", 42, 3, null, "diagnostic", "{\"property\":\"[redacted]\"}"));
        var row = Assert.Single(await fixture.Store.IncidentsAsync(long.MaxValue, 50, default));
        var details = await fixture.Store.IncidentAsync(row.Id, default);
        Assert.Equal(Convert.ToBase64String(raw), details!.RawBodyBase64);
        Assert.Equal("diagnostic", details.Description);
        Assert.Contains("unexpected", details.MetadataJson);
        Assert.Contains("[redacted]", details.MetadataJson);
    }
    private static ValidatedEvent Value(string id = "event-1", string unit = "ms") => ContractRegistry.Validate(
        ContractsTests.Event(id, $$"""{"name":"latency","value":12,"unit":"{{unit}}"}"""), DateTimeOffset.UtcNow);
    private static Delivery Delivery(ValidatedEvent value, string transport = "relay:1") =>
        new(transport, "p", value.Envelope.EventId, System.Text.Encoding.UTF8.GetBytes(value.CanonicalJson), null);

    [Fact, Trait("Category", "Integration")]
    public async Task ConcurrentAdmissionsCreateOnePayloadAndOutbox()
    {
        await fixture.Reset();
        var results = await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => fixture.Store.AdmitAsync("p", Value(), null, default)));
        Assert.Single(results, r => !r.Replay);
        Assert.All(results, r => Assert.False(r.Conflict));
        Assert.Equal(1, await fixture.Count("events"));
        Assert.Equal(1, await fixture.Count("outbox"));
        Assert.Equal(1, await fixture.Count("event_payloads"));
        Assert.True((await fixture.Store.AdmitAsync("p", Value(unit: "seconds"), null, default)).Conflict);
    }
    [Fact, Trait("Category", "Integration")]
    public async Task CompetingConsumersApplyMetricOnce()
    {
        await fixture.Reset(); var value = Value(); await fixture.Store.AdmitAsync("p", value, null, default);
        var outcomes = await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => fixture.Store.ProcessAsync(Delivery(value), default)));
        Assert.Single(outcomes, r => r);
        Assert.Equal(1, await fixture.Count("processed_events")); Assert.Equal(1, await fixture.Count("event_records"));
        await using var c = await fixture.Source.OpenConnectionAsync();
        await using var query = new Npgsql.NpgsqlCommand("SELECT count,sum,min,max FROM rollups_minute", c);
        await using var reader = await query.ExecuteReaderAsync(); await reader.ReadAsync();
        Assert.Equal(1, reader.GetInt64(0)); Assert.Equal(12, reader.GetDouble(1)); Assert.Equal(12, reader.GetDouble(2)); Assert.Equal(12, reader.GetDouble(3));
    }
    [Fact, Trait("Category", "Integration")]
    public async Task CaptureReplayThenOriginalRedeliveryKeepsOneIncident()
    {
        await fixture.Reset(); var value = Value();
        const string parent = "00-11111111111111111111111111111111-2222222222222222-01";
        await fixture.Store.AdmitAsync("p", value, parent, default);
        await fixture.Store.CaptureAsync("relay", "relay:1", Delivery(value), "handler_error", Delivery(value).Body, default);
        var incident = Assert.Single(await fixture.Store.IncidentsAsync(long.MaxValue, 50, default));
        var resolution = await fixture.Store.ResolveAsync(incident.Id, "replay", "p", default);
        Assert.Equal("replay_pending", resolution!.State); Assert.NotEqual(1, resolution.OutboxId);
        await using (var connection = await fixture.Source.OpenConnectionAsync())
        await using (var command = new Npgsql.NpgsqlCommand("SELECT trace_parent FROM outbox WHERE replay_of IS NOT NULL", connection))
            Assert.Equal(parent, (string)(await command.ExecuteScalarAsync())!);
        await fixture.Store.CaptureAsync("relay", "relay:1", Delivery(value), "handler_error", Delivery(value).Body, default);
        Assert.Equal(1, await fixture.Count("incidents")); Assert.Equal(1, await fixture.Count("capture_receipts"));
        await Assert.ThrowsAsync<StateConflictException>(() => fixture.Store.ResolveAsync(incident.Id, "replay", "p", default));
    }
    [Fact, Trait("Category", "Integration")]
    public async Task ProcessedReplayIsAuditedNoOp()
    {
        await fixture.Reset(); var value = Value(); await fixture.Store.AdmitAsync("p", value, null, default);
        await fixture.Store.ProcessAsync(Delivery(value), default);
        await fixture.Store.CaptureAsync("relay", "relay:1", Delivery(value), "settlement_failed", Delivery(value).Body, default);
        var incident = Assert.Single(await fixture.Store.IncidentsAsync(long.MaxValue, 50, default));
        Assert.Equal("processed", (await fixture.Store.ResolveAsync(incident.Id, "replay", "p", default))!.State);
        Assert.Equal(1, await fixture.Count("outbox")); Assert.Equal(1, await fixture.Count("incident_actions"));
    }
    [Fact, Trait("Category", "Integration")]
    public async Task DiscardFencesAlreadyQueuedDuplicates()
    {
        await fixture.Reset(); var value = Value(); await fixture.Store.AdmitAsync("p", value, null, default);
        await fixture.Store.CaptureAsync("relay", "relay:1", Delivery(value), "handler_error", Delivery(value).Body, default);
        var incident = Assert.Single(await fixture.Store.IncidentsAsync(long.MaxValue, 50, default));
        Assert.Equal("discarded", (await fixture.Store.ResolveAsync(incident.Id, "discard", "p", default))!.State);
        Assert.False(await fixture.Store.ProcessAsync(Delivery(value), default));
        Assert.Equal(0, await fixture.Count("processed_events"));
        Assert.Equal("discarded", (await fixture.Store.StatusAsync("p", "event-1", default))!.State);
    }
    [Fact, Trait("Category", "Integration")]
    public async Task RetentionProtectsPendingPayloadAndRetainsCompletionFence()
    {
        await fixture.Reset(); var first = Value(); await fixture.Store.AdmitAsync("p", first, null, default);
        await fixture.Execute("UPDATE events SET received_at=now()-interval '31 days'");
        Assert.Equal(0, await fixture.Store.RetainAsync(default));
        await fixture.Store.ProcessAsync(Delivery(first), default);
        await fixture.Execute("UPDATE outbox SET status='sent',sent_at=now()");
        Assert.Equal(1, await fixture.Store.RetainAsync(default));
        Assert.Equal(1, await fixture.Count("events")); Assert.Equal(1, await fixture.Count("processed_events"));
        Assert.True((await fixture.Store.AdmitAsync("p", first, null, default)).Replay);
        Assert.False(await fixture.Store.ProcessAsync(Delivery(first), default));
    }
    [Fact, Trait("Category", "Integration")]
    public async Task UnknownOrMismatchedPayloadCannotApply()
    {
        await fixture.Reset(); var value = Value();
        await Assert.ThrowsAsync<PermanentDeliveryException>(() => fixture.Store.ProcessAsync(Delivery(value), default));
        await fixture.Store.AdmitAsync("p", value, null, default);
        await Assert.ThrowsAsync<PermanentDeliveryException>(() => fixture.Store.ProcessAsync(Delivery(Value(unit: "seconds")), default));
        Assert.Equal(0, await fixture.Count("processed_events"));
    }
    [Fact, Trait("Category", "Integration")]
    public async Task DispatcherPersistsBackoffAndPermanentQuarantine()
    {
        await fixture.Reset(); await fixture.Store.AdmitAsync("p", Value(), null, default);
        var failing = new Dispatcher(fixture.Source, new ThrowingPublisher(), Options.Create(new RelayOptions()));
        Assert.Equal(1, await failing.RunOnceAsync(default));
        await using (var c = await fixture.Source.OpenConnectionAsync())
        await using (var q = new Npgsql.NpgsqlCommand("SELECT attempts,next_attempt_at>now(),status FROM outbox", c))
        await using (var reader = await q.ExecuteReaderAsync())
        { await reader.ReadAsync(); Assert.Equal(1, reader.GetInt32(0)); Assert.True(reader.GetBoolean(1)); Assert.Equal("pending", reader.GetString(2)); }
        await fixture.Execute("UPDATE outbox SET next_attempt_at=now()");
        var permanent = new Dispatcher(fixture.Source, new QuarantinePublisher(), Options.Create(new RelayOptions()));
        await permanent.RunOnceAsync(default);
        Assert.Equal("dispatch_quarantined", (await fixture.Store.StatusAsync("p", "event-1", default))!.State);
        Assert.Equal(1, await fixture.Count("incidents"));
    }
    [Fact, Trait("Category", "Integration")]
    public async Task TwoDispatchersSkipLockedClaimsWithoutPublishingSameRow()
    {
        await fixture.Reset(); for (var i = 0; i < 20; i++) await fixture.Store.AdmitAsync("p", Value($"event-{i}"), null, default);
        var publisher = new RecordingPublisher(); var opts = Options.Create(new RelayOptions { BatchSize = 10 });
        await Task.WhenAll(new Dispatcher(fixture.Source, publisher, opts).RunOnceAsync(default), new Dispatcher(fixture.Source, publisher, opts).RunOnceAsync(default));
        Assert.Equal(20, publisher.Published.Count); Assert.Equal(20, publisher.Published.Distinct().Count());
    }
    private sealed class ThrowingPublisher : IMessagePublisher
    { public Task<PublishResult> PublishAsync(IReadOnlyList<DispatchItem> items, CancellationToken ct) => throw new TimeoutException(); }
    private sealed class QuarantinePublisher : IMessagePublisher
    { public Task<PublishResult> PublishAsync(IReadOnlyList<DispatchItem> items, CancellationToken ct) => Task.FromResult(new PublishResult(new HashSet<long>(), items.ToDictionary(i => i.Id, _ => "message_too_large"))); }
    private sealed class RecordingPublisher : IMessagePublisher
    {
        public System.Collections.Concurrent.ConcurrentBag<long> Published { get; } = [];
        public async Task<PublishResult> PublishAsync(IReadOnlyList<DispatchItem> items, CancellationToken ct)
        { await Task.Delay(50, ct); foreach (var item in items) Published.Add(item.Id); return new(items.Select(i => i.Id).ToHashSet(), new Dictionary<long, string>()); }
    }
}
