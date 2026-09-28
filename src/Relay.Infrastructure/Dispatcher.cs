using Microsoft.Extensions.Options;
using Npgsql;
using Relay.Core;

namespace Relay.Infrastructure;

public sealed class Dispatcher(NpgsqlDataSource source, IMessagePublisher publisher, IOptions<RelayOptions> options)
{
    public async Task<int> RunOnceAsync(CancellationToken ct)
    {
        await using var connection = await source.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await Sql.Execute(connection, transaction, "SET LOCAL idle_in_transaction_session_timeout='30s'", ct);
        var items = new List<DispatchItem>();
        await using (var command = Sql.Command(connection, transaction, """
            SELECT o.id,o.producer_id,o.event_id,p.body,o.trace_parent,o.attempts,e.type,e.schema_version
            FROM outbox o JOIN event_payloads p USING(producer_id,event_id)
            JOIN events e USING(producer_id,event_id)
            WHERE o.status='pending' AND o.next_attempt_at<=now()
            ORDER BY o.next_attempt_at,o.id LIMIT @batch FOR UPDATE OF o SKIP LOCKED
            """, ("batch", options.Value.BatchSize)))
        await using (var reader = await command.ExecuteReaderAsync(ct))
            while (await reader.ReadAsync(ct)) items.Add(new(reader.GetInt64(0), reader.GetString(1), reader.GetString(2),
                reader.GetString(3), reader.IsDBNull(4) ? null : reader.GetString(4), reader.GetInt32(5), reader.GetString(6), reader.GetInt32(7)));
        if (items.Count == 0) { await transaction.CommitAsync(ct); return 0; }
        PublishResult result;
        try
        {
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
            deadline.CancelAfter(TimeSpan.FromSeconds(10));
            result = await publisher.PublishAsync(items, deadline.Token);
            Failpoints.Hit("dispatcher.after_publish");
        }
        catch (Exception exception) when (!ct.IsCancellationRequested && exception is not NpgsqlException)
        {
            Telemetry.DispatchFailures.Add(1, new KeyValuePair<string, object?>("kind", "transient"));
            foreach (var item in items)
            {
                var delay = Math.Min(30, 0.2 * Math.Pow(2, Math.Min(item.Attempts, 8))) + Random.Shared.NextDouble() * 0.2;
                await Sql.Execute(connection, transaction,
                    "UPDATE outbox SET attempts=attempts+1,next_attempt_at=clock_timestamp()+@delay,last_error='publish_uncertain' WHERE id=@id", ct,
                    ("delay", TimeSpan.FromSeconds(delay)), ("id", item.Id));
            }
            await transaction.CommitAsync(ct);
            return items.Count;
        }
        foreach (var item in items)
        {
            if (result.Sent.Contains(item.Id))
                await Sql.Execute(connection, transaction, "UPDATE outbox SET status='sent',sent_at=clock_timestamp(),attempts=attempts+1,last_error=NULL WHERE id=@id", ct, ("id", item.Id));
            else if (result.Quarantined.TryGetValue(item.Id, out var reason))
            {
                await Sql.Execute(connection, transaction, "UPDATE outbox SET status='quarantined',attempts=attempts+1,last_error=@reason WHERE id=@id", ct,
                    ("reason", reason), ("id", item.Id));
                await Sql.Execute(connection, transaction, """
                    INSERT INTO incidents(producer_id,event_id,source,receipt_id,reason,raw_body)
                    VALUES(@p,@e,'dispatch',@receipt,@reason,@body)
                    """, ct, ("p", item.ProducerId), ("e", item.EventId), ("receipt", item.TransportId),
                    ("reason", reason), ("body", System.Text.Encoding.UTF8.GetBytes(item.Payload)));
                Telemetry.DispatchFailures.Add(1, new KeyValuePair<string, object?>("kind", "permanent"));
            }
            else throw new InvalidOperationException("Publisher returned an incomplete outcome");
        }
        await transaction.CommitAsync(ct);
        return items.Count;
    }
}
