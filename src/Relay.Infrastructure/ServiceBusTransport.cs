using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;
using Relay.Core;

namespace Relay.Infrastructure;

public static class ServiceBusTransport
{
    public static async Task ProbeAsync(ServiceBusClient client, RelayOptions options, CancellationToken ct)
    {
        if (options.ReadinessUsesReceiver)
        {
            await using var receiver = client.CreateReceiver(options.Queue);
            await receiver.PeekMessageAsync(cancellationToken: ct);
        }
        if (options.ParsedRoles.Contains("dispatcher"))
        {
            await using var sender = client.CreateSender(options.Queue);
            using var batch = await sender.CreateMessageBatchAsync(ct);
        }
    }
    public static CaptureMetadata Metadata(ServiceBusReceivedMessage message)
    {
        var allowed = new[] { "producerId", "eventId", "schemaVersion", "Diagnostic-Id", "Relay-Traceparent" };
        var properties = new Dictionary<string, string?>();
        foreach (var property in message.ApplicationProperties.Take(32))
        {
            var value = property.Value?.ToString();
            properties[property.Key[..Math.Min(property.Key.Length, 128)]] = allowed.Contains(property.Key)
                ? value?[..Math.Min(value.Length, 256)] : "[redacted]";
        }
        var description = message.DeadLetterErrorDescription;
        if (description is not null)
        {
            description = description[..Math.Min(description.Length, 1024)];
            description = System.Text.RegularExpressions.Regex.Replace(description, "[A-Fa-f0-9]{64}", "[redacted]");
        }
        return new(message.MessageId, message.SequenceNumber, message.DeliveryCount,
            TraceParent(message.ApplicationProperties.GetValueOrDefault("Relay-Traceparent") as string), description,
            System.Text.Json.JsonSerializer.Serialize(properties));
    }
    public static ServiceBusClient Client(RelayOptions options)
    {
        var clientOptions = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            RetryOptions = new() { MaxRetries = 1, TryTimeout = TimeSpan.FromSeconds(4), Delay = TimeSpan.FromMilliseconds(200), MaxDelay = TimeSpan.FromSeconds(1) }
        };
        return string.IsNullOrWhiteSpace(options.ServiceBusConnectionString)
            ? new(options.ServiceBusNamespace, new DefaultAzureCredential(), clientOptions)
            : new(options.ServiceBusConnectionString, clientOptions);
    }
    public static Delivery? Delivery(ServiceBusReceivedMessage message)
    {
        if (!message.ApplicationProperties.TryGetValue("producerId", out var p) || p is not string producer ||
            !message.ApplicationProperties.TryGetValue("eventId", out var e) || e is not string eventId ||
            producer.Length > 128 || eventId.Length > 128) return null;
        var trace = TraceParent(message.ApplicationProperties.GetValueOrDefault("Relay-Traceparent") as string);
        return new(message.MessageId, producer, eventId, message.Body.ToMemory(), trace);
    }
    public static string? TraceParent(string? value) => value?.Length == 55 &&
        System.Diagnostics.ActivityContext.TryParse(value, null, true, out _) ? value : null;
}

public sealed class ServiceBusPublisher(ServiceBusClient client, IOptions<RelayOptions> options) : IMessagePublisher, IAsyncDisposable
{
    private readonly ServiceBusSender _sender = client.CreateSender(options.Value.Queue);
    public async Task<PublishResult> PublishAsync(IReadOnlyList<DispatchItem> items, CancellationToken cancellationToken)
    {
        var sent = new HashSet<long>();
        var quarantined = new Dictionary<long, string>();
        ServiceBusMessageBatch batch = await _sender.CreateMessageBatchAsync(cancellationToken);
        var ids = new List<long>();
        try
        {
            foreach (var item in items)
            {
                using var span = Telemetry.Activities.StartActivity("dispatch.enqueue", System.Diagnostics.ActivityKind.Producer, Telemetry.Parent(item.TraceParent));
                var message = new ServiceBusMessage(BinaryData.FromString(item.Payload))
                {
                    MessageId = item.TransportId,
                    Subject = item.Type,
                    ContentType = "application/json",
                    TimeToLive = TimeSpan.FromDays(7)
                };
                message.ApplicationProperties["producerId"] = item.ProducerId;
                message.ApplicationProperties["eventId"] = item.EventId;
                message.ApplicationProperties["schemaVersion"] = item.SchemaVersion;
                if ((span?.Id ?? item.TraceParent) is { } parent)
                {
                    message.ApplicationProperties["Relay-Traceparent"] = parent;
                    message.ApplicationProperties["Diagnostic-Id"] = parent;
                }
                if (!batch.TryAddMessage(message))
                {
                    if (ids.Count > 0)
                    {
                        await _sender.SendMessagesAsync(batch, cancellationToken);
                        sent.UnionWith(ids);
                        batch.Dispose();
                        batch = await _sender.CreateMessageBatchAsync(cancellationToken);
                        ids.Clear();
                    }
                    if (!batch.TryAddMessage(message)) { quarantined.Add(item.Id, "message_too_large"); continue; }
                }
                ids.Add(item.Id);
            }
            if (ids.Count > 0) { await _sender.SendMessagesAsync(batch, cancellationToken); sent.UnionWith(ids); }
            return new(sent, quarantined);
        }
        finally { batch.Dispose(); }
    }
    public ValueTask DisposeAsync() => _sender.DisposeAsync();
}
