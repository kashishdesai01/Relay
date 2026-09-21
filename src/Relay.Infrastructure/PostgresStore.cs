using System.Security.Cryptography;
using System.Text;
using Npgsql;
using Relay.Contracts;
using Relay.Core;

namespace Relay.Infrastructure;

public sealed class PostgresStore(NpgsqlDataSource source, TimeProvider clock) : IRelayStore
{
    public async Task<Admission> AdmitAsync(string producerId, ValidatedEvent value, string? traceParent, CancellationToken ct)
    {
        await using var connection = await source.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        var e = value.Envelope;
        var inserted = await Sql.Execute(connection, transaction, """
            INSERT INTO events(producer_id,event_id,type,schema_version,payload_hash,trace_parent)
            VALUES(@p,@e,@type,@version,@hash,@trace) ON CONFLICT DO NOTHING
            """, ct, ("p", producerId), ("e", e.EventId), ("type", e.Type), ("version", e.SchemaVersion), ("hash", value.Hash), ("trace", traceParent));
        if (inserted == 0)
        {
            var hash = (byte[])(await Sql.Scalar(connection, transaction,
                "SELECT payload_hash FROM events WHERE producer_id=@p AND event_id=@e", ct, ("p", producerId), ("e", e.EventId)))!;
            await transaction.CommitAsync(ct);
            return new(true, !CryptographicOperations.FixedTimeEquals(hash, value.Hash));
        }
        await Sql.Execute(connection, transaction, "INSERT INTO event_payloads VALUES(@p,@e,@body)", ct,
            ("p", producerId), ("e", e.EventId), ("body", value.CanonicalJson));
        await Sql.Execute(connection, transaction,
            "INSERT INTO outbox(producer_id,event_id,trace_parent) VALUES(@p,@e,@trace)", ct,
            ("p", producerId), ("e", e.EventId), ("trace", traceParent));
        Failpoints.Hit("api.before_commit");
        await transaction.CommitAsync(ct);
        Failpoints.Hit("api.after_commit");
        return new(false, false);
    }

    public async Task<Producer?> AuthenticateAsync(byte[] hash, CancellationToken ct)
    {
        await using var connection = await source.OpenConnectionAsync(ct);
        await using var command = Sql.Command(connection, null,
            "SELECT id,scopes,key_hash FROM producers WHERE key_hash=@hash AND NOT disabled", ("hash", hash));
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct) || !CryptographicOperations.FixedTimeEquals(reader.GetFieldValue<byte[]>(2), hash)) return null;
        return new(reader.GetString(0), reader.GetFieldValue<string[]>(1));
    }

    public async Task<EventStatus?> StatusAsync(string producerId, string eventId, CancellationToken ct)
    {
        await using var connection = await source.OpenConnectionAsync(ct);
        await using var command = Sql.Command(connection, null, """
            SELECT e.received_at,e.processed_at,
            EXISTS(SELECT 1 FROM event_payloads p WHERE p.producer_id=e.producer_id AND p.event_id=e.event_id),
            CASE WHEN e.processed_at IS NOT NULL THEN 'processed'
                 WHEN e.discarded_at IS NOT NULL THEN 'discarded'
                 WHEN EXISTS(SELECT 1 FROM incidents i WHERE i.producer_id=e.producer_id AND i.event_id=e.event_id AND i.state='captured' AND i.source='dispatch') THEN 'dispatch_quarantined'
                 WHEN EXISTS(SELECT 1 FROM incidents i WHERE i.producer_id=e.producer_id AND i.event_id=e.event_id AND i.state='captured') THEN 'consumer_quarantined'
                 WHEN EXISTS(SELECT 1 FROM outbox o WHERE o.producer_id=e.producer_id AND o.event_id=e.event_id AND o.status='pending' AND o.replay_of IS NOT NULL) THEN 'replay_pending'
                 WHEN EXISTS(SELECT 1 FROM outbox o WHERE o.producer_id=e.producer_id AND o.event_id=e.event_id AND o.status='sent') THEN 'dispatched'
                 ELSE 'accepted' END
            FROM events e WHERE e.producer_id=@p AND e.event_id=@e
            """, ("p", producerId), ("e", eventId));
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        return new(eventId, reader.GetString(3), reader.GetFieldValue<DateTimeOffset>(0),
            reader.IsDBNull(1) ? null : reader.GetFieldValue<DateTimeOffset>(1), reader.GetBoolean(2));
    }

    public async Task<bool> ProcessAsync(Delivery delivery, CancellationToken ct)
    {
        var validated = ContractRegistry.Validate(delivery.Body, clock.GetUtcNow());
        if (validated.Envelope.EventId != delivery.EventId) throw new PermanentDeliveryException("identity_mismatch");
        await using var connection = await source.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        byte[] storedHash;
        bool discarded;
        await using (var command = Sql.Command(connection, transaction,
            "SELECT payload_hash,discarded_at IS NOT NULL FROM events WHERE producer_id=@p AND event_id=@e FOR NO KEY UPDATE",
            ("p", delivery.ProducerId), ("e", delivery.EventId)))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            if (!await reader.ReadAsync(ct)) throw new PermanentDeliveryException("unknown_admission");
            storedHash = reader.GetFieldValue<byte[]>(0);
            discarded = reader.GetBoolean(1);
        }
        if (!CryptographicOperations.FixedTimeEquals(storedHash, validated.Hash)) throw new PermanentDeliveryException("payload_mismatch");
        if (discarded) { await transaction.CommitAsync(ct); return false; }
        var inserted = await Sql.Execute(connection, transaction,
            "INSERT INTO processed_events(producer_id,event_id) VALUES(@p,@e) ON CONFLICT DO NOTHING", ct,
            ("p", delivery.ProducerId), ("e", delivery.EventId));
#if FAILPOINTS
        if (Failpoints.IsEnabled("consumer.skip_dedupe")) inserted = 1;
#endif
        if (inserted == 0) { await transaction.CommitAsync(ct); return false; }
        var recordSql = "INSERT INTO event_records(producer_id,event_id,type) VALUES(@p,@e,@type)";
#if FAILPOINTS
        if (Failpoints.IsEnabled("consumer.skip_dedupe")) recordSql += " ON CONFLICT DO NOTHING";
#endif
        await Sql.Execute(connection, transaction,
            recordSql, ct,
            ("p", delivery.ProducerId), ("e", delivery.EventId), ("type", validated.Envelope.Type));
        if (validated.Metric is { } metric)
        {
            var time = validated.Envelope.OccurredAt.UtcDateTime;
            var bucket = new DateTime(time.Year, time.Month, time.Day, time.Hour, time.Minute, 0, DateTimeKind.Utc);
            await Sql.Execute(connection, transaction, """
                INSERT INTO rollups_minute(producer_id,metric_name,unit,bucket_start,count,sum,min,max)
                VALUES(@p,@name,@unit,@bucket,1,@v,@v,@v)
                ON CONFLICT(producer_id,metric_name,unit,bucket_start) DO UPDATE SET
                 count=rollups_minute.count+1,sum=rollups_minute.sum+@v,
                 min=LEAST(rollups_minute.min,@v),max=GREATEST(rollups_minute.max,@v)
                """, ct, ("p", delivery.ProducerId), ("name", metric.Name), ("unit", metric.Unit), ("bucket", bucket), ("v", metric.Value));
        }
        await Sql.Execute(connection, transaction,
            "UPDATE events SET processed_at=now() WHERE producer_id=@p AND event_id=@e", ct,
            ("p", delivery.ProducerId), ("e", delivery.EventId));
#if FAILPOINTS
        await Failpoints.PauseAsync(ct);
#endif
        Failpoints.Hit("consumer.permanent_failure");
        Failpoints.Hit("consumer.before_commit");
        await transaction.CommitAsync(ct);
        Failpoints.Hit("consumer.after_commit");
        return true;
    }

    public async Task CaptureAsync(string sourceQueue, string receiptId, Delivery? delivery, string reason,
        ReadOnlyMemory<byte> rawBody, CancellationToken ct, CaptureMetadata? metadata = null)
    {
        await using var connection = await source.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        var receipt = await Sql.Execute(connection, transaction,
            "INSERT INTO capture_receipts(source_queue,receipt_id) VALUES(@q,@r) ON CONFLICT DO NOTHING", ct,
            ("q", sourceQueue), ("r", receiptId));
        if (receipt == 0) { await transaction.CommitAsync(ct); return; }
        var known = delivery is not null && (bool)(await Sql.Scalar(connection, transaction,
            "SELECT EXISTS(SELECT 1 FROM events WHERE producer_id=@p AND event_id=@e)", ct,
            ("p", delivery.ProducerId), ("e", delivery.EventId)))!;
        await Sql.Execute(connection, transaction, """
            INSERT INTO incidents(producer_id,event_id,source,receipt_id,reason,raw_body,metadata_json,description)
            VALUES(@p,@e,@source,@r,@reason,@body,@metadata,@description)
            """, ct, ("p", known ? delivery!.ProducerId : null), ("e", known ? delivery!.EventId : null),
            ("source", known ? "consumer" : "transport"), ("r", receiptId), ("reason", SafeReason(reason)),
            ("body", rawBody[..Math.Min(rawBody.Length, 256 * 1024)].ToArray()),
            ("metadata", metadata is null ? "{}" : System.Text.Json.JsonSerializer.Serialize(metadata)),
            ("description", metadata?.Description));
        await transaction.CommitAsync(ct);
        Failpoints.Hit("capture.after_insert");
    }

    public async Task<IReadOnlyList<Incident>> IncidentsAsync(long before, int limit, CancellationToken ct)
    {
        await using var connection = await source.OpenConnectionAsync(ct);
        await using var command = Sql.Command(connection, null, """
            SELECT id,producer_id,event_id,source,reason,state,captured_at FROM incidents
            WHERE id<@before ORDER BY id DESC LIMIT @limit
            """, ("before", before), ("limit", limit));
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<Incident>();
        while (await reader.ReadAsync(ct)) rows.Add(new(reader.GetInt64(0), reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2), reader.GetString(3), reader.GetString(4), reader.GetString(5), reader.GetFieldValue<DateTimeOffset>(6)));
        return rows;
    }

    public async Task<IncidentDetails?> IncidentAsync(long id, CancellationToken ct)
    {
        await using var connection = await source.OpenConnectionAsync(ct);
        await using var command = Sql.Command(connection, null, """
            SELECT id,producer_id,event_id,source,reason,state,captured_at,raw_body,metadata_json,description
            FROM incidents WHERE id=@id
            """, ("id", id));
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        return new(new(reader.GetInt64(0), reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2), reader.GetString(3), reader.GetString(4), reader.GetString(5),
            reader.GetFieldValue<DateTimeOffset>(6)), Convert.ToBase64String(reader.GetFieldValue<byte[]>(7)),
            reader.GetString(8), reader.IsDBNull(9) ? null : reader.GetString(9));
    }

    public async Task<Resolution?> ResolveAsync(long id, string action, string actor, CancellationToken ct)
    {
        if (action is not ("replay" or "discard")) throw new StateConflictException("invalid_action");
        await using var connection = await source.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        string? producerId, eventId;
        await using (var command = Sql.Command(connection, transaction,
            "SELECT producer_id,event_id,state FROM incidents WHERE id=@id FOR NO KEY UPDATE", ("id", id)))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            if (!await reader.ReadAsync(ct)) return null;
            if (reader.GetString(2) != "captured") throw new StateConflictException("incident_already_resolved");
            producerId = reader.IsDBNull(0) ? null : reader.GetString(0);
            eventId = reader.IsDBNull(1) ? null : reader.GetString(1);
        }
        string state = "discarded";
        long? outboxId = null;
        if (producerId is not null)
        {
            bool processed, discarded;
            await using (var command = Sql.Command(connection, transaction,
                "SELECT processed_at IS NOT NULL,discarded_at IS NOT NULL FROM events WHERE producer_id=@p AND event_id=@e FOR NO KEY UPDATE",
                ("p", producerId), ("e", eventId)))
            await using (var reader = await command.ExecuteReaderAsync(ct))
            { await reader.ReadAsync(ct); processed = reader.GetBoolean(0); discarded = reader.GetBoolean(1); }
            if (processed) state = "processed";
            else if (action == "replay")
            {
                if (discarded) throw new StateConflictException("event_discarded");
                if (!(bool)(await Sql.Scalar(connection, transaction,
                    "SELECT EXISTS(SELECT 1 FROM event_payloads WHERE producer_id=@p AND event_id=@e)", ct, ("p", producerId), ("e", eventId)))!)
                    throw new StateConflictException("payload_unavailable");
                state = "replay_pending";
                outboxId = (long)(await Sql.Scalar(connection, transaction,
                    "INSERT INTO outbox(producer_id,event_id,replay_of,trace_parent) SELECT @p,@e,@id,trace_parent FROM events WHERE producer_id=@p AND event_id=@e RETURNING id", ct,
                    ("p", producerId), ("e", eventId), ("id", id)))!;
            }
            else
            {
                await Sql.Execute(connection, transaction,
                    "UPDATE events SET discarded_at=now() WHERE producer_id=@p AND event_id=@e", ct, ("p", producerId), ("e", eventId));
                await Sql.Execute(connection, transaction,
                    "UPDATE outbox SET status='discarded' WHERE producer_id=@p AND event_id=@e AND status IN ('pending','quarantined')", ct,
                    ("p", producerId), ("e", eventId));
            }
        }
        else if (action == "replay") throw new StateConflictException("transport_incident_requires_investigation");
        await Sql.Execute(connection, transaction, "UPDATE incidents SET state=@state WHERE id=@id", ct, ("state", state), ("id", id));
        await Sql.Execute(connection, transaction, "INSERT INTO incident_actions(incident_id,action,actor) VALUES(@id,@action,@actor)", ct,
            ("id", id), ("action", action), ("actor", actor));
        Failpoints.Hit("replay.before_commit");
        await transaction.CommitAsync(ct);
        return new(id, state, outboxId);
    }

    public async Task<Backlog> BacklogAsync(CancellationToken ct)
    {
        await using var connection = await source.OpenConnectionAsync(ct);
        await using var command = Sql.Command(connection, null,
            "SELECT count(*),COALESCE(EXTRACT(EPOCH FROM now()-min(created_at)),0)::double precision FROM outbox WHERE status='pending'");
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new(reader.GetInt64(0), reader.GetDouble(1));
    }
    public async Task<bool> ProbeAsync(CancellationToken ct)
    {
        try { await using var c = await source.OpenConnectionAsync(ct); return (int)(await Sql.Scalar(c, null, "SELECT 1", ct))! == 1; }
        catch (NpgsqlException) { return false; }
    }
    public async Task<int> RetainAsync(CancellationToken ct)
    {
        await using var connection = await source.OpenConnectionAsync(ct);
        return await Sql.Execute(connection, null, """
            DELETE FROM event_payloads p USING (
             SELECT e.producer_id,e.event_id FROM events e
             WHERE e.received_at<now()-interval '30 days' AND (e.processed_at IS NOT NULL OR e.discarded_at IS NOT NULL)
             AND NOT EXISTS(SELECT 1 FROM outbox o WHERE o.producer_id=e.producer_id AND o.event_id=e.event_id AND o.status IN ('pending','quarantined'))
             AND NOT EXISTS(SELECT 1 FROM incidents i WHERE i.producer_id=e.producer_id AND i.event_id=e.event_id AND i.state='captured')
             ORDER BY e.received_at LIMIT 1000 FOR NO KEY UPDATE OF e SKIP LOCKED
            ) eligible WHERE p.producer_id=eligible.producer_id AND p.event_id=eligible.event_id
            """, ct);
    }
    internal static string SafeReason(string reason) => reason.Length <= 128 && reason.All(c => char.IsAsciiLetterOrDigit(c) || c is '_' or '.') ? reason : "broker_failure";
}
