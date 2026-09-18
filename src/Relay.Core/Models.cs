using Relay.Contracts;

namespace Relay.Core;

public sealed record Admission(bool Replay, bool Conflict);
public sealed record Producer(string Id, string[] Scopes);
public sealed record DispatchItem(long Id, string ProducerId, string EventId, string Payload, string? TraceParent, int Attempts,
    string Type = "unknown", int SchemaVersion = 0)
{
    public string TransportId => $"relay:{Id}";
}
public sealed record Delivery(string TransportId, string ProducerId, string EventId, ReadOnlyMemory<byte> Body, string? TraceParent);
public sealed record Incident(long Id, string? ProducerId, string? EventId, string Source, string Reason, string State, DateTimeOffset CapturedAt);
public sealed record EventStatus(string EventId, string State, DateTimeOffset AcceptedAt, DateTimeOffset? ProcessedAt, bool PayloadAvailable);
public sealed record Resolution(long IncidentId, string State, long? OutboxId);
public sealed record Backlog(long Count, double OldestSeconds);
public sealed record CaptureMetadata(string TransportId, long SequenceNumber, int DeliveryCount, string? TraceParent,
    string? Description, string PropertiesJson);
public sealed record IncidentDetails(Incident Incident, string RawBodyBase64, string MetadataJson, string? Description);
public sealed class StateConflictException(string code) : Exception(code);
public sealed class PermanentDeliveryException(string code) : Exception(code);

public interface IMessagePublisher
{
    // Only IDs in this return value are confirmed accepted. An exception leaves the outcome uncertain.
    Task<PublishResult> PublishAsync(IReadOnlyList<DispatchItem> items, CancellationToken cancellationToken);
}
public sealed record PublishResult(IReadOnlySet<long> Sent, IReadOnlyDictionary<long, string> Quarantined);

public interface IRelayStore
{
    Task<Admission> AdmitAsync(string producerId, ValidatedEvent value, string? traceParent, CancellationToken ct);
    Task<Producer?> AuthenticateAsync(byte[] hash, CancellationToken ct);
    Task<EventStatus?> StatusAsync(string producerId, string eventId, CancellationToken ct);
    Task<bool> ProcessAsync(Delivery delivery, CancellationToken ct);
    Task CaptureAsync(string sourceQueue, string receiptId, Delivery? delivery, string reason, ReadOnlyMemory<byte> rawBody, CancellationToken ct, CaptureMetadata? metadata = null);
    Task<IReadOnlyList<Incident>> IncidentsAsync(long before, int limit, CancellationToken ct);
    Task<IncidentDetails?> IncidentAsync(long id, CancellationToken ct);
    Task<Resolution?> ResolveAsync(long id, string action, string actor, CancellationToken ct);
    Task<Backlog> BacklogAsync(CancellationToken ct);
    Task<bool> ProbeAsync(CancellationToken ct);
    Task<int> RetainAsync(CancellationToken ct);
}
