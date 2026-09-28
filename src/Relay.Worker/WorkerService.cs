using System.Diagnostics;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;
using Npgsql;
using Relay.Contracts;
using Relay.Core;
using Relay.Infrastructure;

namespace Relay.Worker;

public sealed class WorkerService(IRelayStore store, Dispatcher dispatcher, ServiceBusClient client,
    IOptions<RelayOptions> options, ILogger<WorkerService> logger) : BackgroundService
{
    private int _pauseRequested;
    private int _running;
    public bool ConsumerRunning => Volatile.Read(ref _running) == 1;
    public bool HasConsumer => options.Value.ParsedRoles.Contains("consumer", StringComparer.Ordinal);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var roles = options.Value.ParsedRoles;
        if (roles.Length == 0 || roles.Any(r => r is not ("dispatcher" or "consumer" or "dlq-capture" or "retention")))
            throw new InvalidOperationException("Unknown worker role");
        var tasks = new List<Task>();
        if (roles.Contains("dispatcher")) tasks.Add(DispatchLoop(stoppingToken));
        if (roles.Contains("consumer")) tasks.Add(SuperviseConsumer(stoppingToken));
        if (roles.Contains("dlq-capture")) tasks.Add(CaptureLoop(stoppingToken));
        if (roles.Contains("retention")) tasks.Add(RetentionLoop(stoppingToken));
        await Task.WhenAll(tasks);
    }

    private async Task DispatchLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try { var count = await dispatcher.RunOnceAsync(ct); await Task.Delay(count == 0 ? 250 : 25, ct); }
            catch (Exception e) when (!ct.IsCancellationRequested)
            {
                logger.LogWarning("Dispatch delayed; error category {Category}", e.GetType().Name);
                await Task.Delay(1000, ct);
            }
        }
    }

    // This task alone owns processor lifecycle. A callback never awaits StopProcessingAsync.
    private async Task SuperviseConsumer(CancellationToken ct)
    {
        await using var processor = client.CreateProcessor(options.Value.Queue, new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false,
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            MaxConcurrentCalls = 8,
            PrefetchCount = 0,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(5)
        });
        processor.ProcessMessageAsync += Consume;
        processor.ProcessErrorAsync += args =>
        {
            logger.LogWarning("Broker operation delayed; error category {Category}", args.Exception.GetType().Name);
            return Task.CompletedTask;
        };
        var gauge = Telemetry.Meter.CreateObservableGauge("relay_breaker_state", () => Volatile.Read(ref _pauseRequested));
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var healthy = await store.ProbeAsync(ct);
                if ((!healthy || Volatile.Read(ref _pauseRequested) == 1) && processor.IsProcessing)
                {
                    Interlocked.Exchange(ref _running, 0);
                    await processor.StopProcessingAsync(ct);
                }
                if (healthy && !processor.IsProcessing)
                {
                    Interlocked.Exchange(ref _pauseRequested, 0);
                    await processor.StartProcessingAsync(ct);
                    Interlocked.Exchange(ref _running, 1);
                }
                await Task.Delay(1000, ct);
            }
        }
        finally
        {
            Interlocked.Exchange(ref _running, 0);
            using var shutdown = new CancellationTokenSource(TimeSpan.FromSeconds(45));
            await processor.StopProcessingAsync(shutdown.Token);
        }
    }

    private async Task Consume(ProcessMessageEventArgs args)
    {
        var delivery = ServiceBusTransport.Delivery(args.Message);
        using var span = Telemetry.Activities.StartActivity("consumer.apply", ActivityKind.Consumer, Telemetry.Parent(delivery?.TraceParent));
        var started = Stopwatch.GetTimestamp();
        if (delivery is null) { await args.DeadLetterMessageAsync(args.Message, "invalid_transport", cancellationToken: args.CancellationToken); return; }
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(args.CancellationToken);
        budget.CancelAfter(TimeSpan.FromSeconds(10));
        try
        {
            bool applied = false;
            for (var attempt = 0; ; attempt++)
            {
                try { applied = await store.ProcessAsync(delivery, budget.Token); break; }
                catch (Exception e) when (attempt < 2 && e is NpgsqlException or TimeoutException)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(200 * Math.Pow(2, attempt) + Random.Shared.Next(100)), budget.Token);
                }
            }
            Telemetry.Consumed.Add(1, new KeyValuePair<string, object?>("outcome", applied ? "applied" : "duplicate_or_discarded"));
            if (applied)
            {
                var status = await store.StatusAsync(delivery.ProducerId, delivery.EventId, budget.Token);
                if (status?.ProcessedAt is { } processed)
                    Telemetry.EndToEnd.Record((processed - status.AcceptedAt).TotalSeconds);
            }
            try { await args.CompleteMessageAsync(args.Message, args.CancellationToken); }
            catch (ServiceBusException e) when (e.Reason == ServiceBusFailureReason.MessageLockLost)
            { logger.LogInformation("Settlement lock lost after commit; redelivery remains idempotent"); }
        }
        catch (Exception e) when (e is ContractException or PermanentDeliveryException)
        {
            var reason = e is ContractException contract ? contract.Code : e.Message;
            Telemetry.Consumed.Add(1, new KeyValuePair<string, object?>("outcome", "permanent"));
            await args.DeadLetterMessageAsync(args.Message, reason, cancellationToken: args.CancellationToken);
        }
        catch (Exception e) when (!args.CancellationToken.IsCancellationRequested)
        {
            if (e is NpgsqlException or TimeoutException or OperationCanceledException) Interlocked.Exchange(ref _pauseRequested, 1);
            Telemetry.Consumed.Add(1, new KeyValuePair<string, object?>("outcome", "transient"));
            logger.LogWarning("Message will retry; error category {Category}", e.GetType().Name);
            await args.AbandonMessageAsync(args.Message, cancellationToken: args.CancellationToken);
        }
        finally { Telemetry.ConsumeDuration.Record(Stopwatch.GetElapsedTime(started).TotalSeconds); }
    }

    private async Task CaptureLoop(CancellationToken ct)
    {
        await using var receiver = client.CreateReceiver(options.Value.Queue, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1), ct);
                if (message is null) continue;
                var delivery = ServiceBusTransport.Delivery(message);
                // Relay transport IDs are stable across duplicate physical sends. For foreign messages,
                // broker sequence identifies this physical receipt; namespace/queue lifecycle must be pinned.
                var receipt = message.MessageId.StartsWith("relay:", StringComparison.Ordinal)
                    ? message.MessageId : $"foreign:{message.SequenceNumber}";
                await store.CaptureAsync($"{options.Value.BrokerSourceId}/{options.Value.Queue}", receipt, delivery,
                    message.DeadLetterReason ?? "unknown", message.Body.ToMemory(), ct, ServiceBusTransport.Metadata(message));
                await receiver.CompleteMessageAsync(message, ct);
            }
            catch (Exception e) when (!ct.IsCancellationRequested)
            {
                logger.LogWarning("DLQ capture delayed; error category {Category}", e.GetType().Name);
                await Task.Delay(1000, ct);
            }
        }
    }
    private async Task RetentionLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try { await store.RetainAsync(ct); }
            catch (Exception e) when (!ct.IsCancellationRequested) { logger.LogWarning("Retention delayed; error category {Category}", e.GetType().Name); }
            await Task.Delay(TimeSpan.FromMinutes(5), ct);
        }
    }
}
