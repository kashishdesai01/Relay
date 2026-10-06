using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;
using Relay.Core;
using Relay.Infrastructure;
using Xunit;

namespace Relay.Tests;

[Collection("Database")]
public sealed class BrokerTests
{
    [Fact, Trait("Category", "Broker")]
    public async Task RoleAwareReadinessUsesLiveBrokerOperationsWithoutSendingMessages()
    {
        await using var client = Client();
        foreach (var roles in new[] { "dispatcher", "consumer", "dlq-capture", "retention", "dispatcher,consumer", "dispatcher,dlq-capture" })
            await ServiceBusTransport.ProbeAsync(client, new RelayOptions { Queue = "relay-tests", Roles = roles }, default);
    }
    private static ServiceBusClient Client()
    {
        var connection = Environment.GetEnvironmentVariable("RELAY_TEST_SERVICEBUS") ?? throw new InvalidOperationException(
            "Broker tests require RELAY_TEST_SERVICEBUS; start the approved emulator stack.");
        return ServiceBusTransport.Client(new RelayOptions { ServiceBusConnectionString = connection });
    }
    private static async Task Purge(ServiceBusClient client, string queue)
    {
        using var budget = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        foreach (var subqueue in new[] { SubQueue.None, SubQueue.DeadLetter })
        {
            await using var receiver = client.CreateReceiver(queue, new ServiceBusReceiverOptions { SubQueue = subqueue });
            while (true)
            {
                var messages = await receiver.ReceiveMessagesAsync(100, TimeSpan.FromMilliseconds(250), budget.Token);
                if (messages.Count == 0) break;
                foreach (var message in messages) await receiver.CompleteMessageAsync(message, budget.Token);
            }
        }
    }
    [Fact, Trait("Category", "Broker")]
    public async Task DuplicateDetectionAndIntentionalReplayUseDifferentTransportIds()
    {
        await using var client = Client(); await Purge(client, "relay-tests");
        await using var publisher = new ServiceBusPublisher(client, Options.Create(new RelayOptions { Queue = "relay-tests" }));
        var id = DateTime.UtcNow.Ticks;
        var original = new DispatchItem(id, "p", "replay-demo", "{}", null, 0);
        await publisher.PublishAsync([original], default);
        await using var receiver = client.CreateReceiver("relay-tests");
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10)); Assert.NotNull(first);
        await receiver.DeadLetterMessageAsync(first, "test_poison", "intentional failure");
        await publisher.PublishAsync([original], default);
        Assert.Null(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));
        await publisher.PublishAsync([original with { Id = id + 1 }], default);
        var replay = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10)); Assert.NotNull(replay);
        Assert.Equal(original.EventId, ServiceBusTransport.Delivery(replay)!.EventId);
        Assert.NotEqual(first.MessageId, replay.MessageId); await receiver.CompleteMessageAsync(replay);
        await using var dlq = client.CreateReceiver("relay-tests", new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        var dead = await dlq.ReceiveMessageAsync(TimeSpan.FromSeconds(10)); Assert.NotNull(dead);
        Assert.Equal("test_poison", dead.DeadLetterReason); Assert.Equal("intentional failure", dead.DeadLetterErrorDescription);
        await dlq.CompleteMessageAsync(dead);
    }
    [Fact, Trait("Category", "Broker")]
    public async Task AbandonAndLockExpiryReachRecoverableDeadLetter()
    {
        await using var client = Client(); await Purge(client, "relay-tests");
        await using var sender = client.CreateSender("relay-tests");
        await sender.SendMessageAsync(new("{}") { MessageId = Guid.NewGuid().ToString() });
        await using var receiver = client.CreateReceiver("relay-tests");
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10)); Assert.NotNull(first);
        var initial = first.DeliveryCount;
        await receiver.AbandonMessageAsync(first);
        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10)); Assert.NotNull(second);
        Assert.True(second.DeliveryCount > initial);
        await Task.Delay(TimeSpan.FromSeconds(6));
        for (var attempt = 0; attempt < 4; attempt++)
        {
            var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1));
            if (message is null) break;
            await receiver.AbandonMessageAsync(message);
        }
        await using var dlq = client.CreateReceiver("relay-tests", new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        var dead = await dlq.ReceiveMessageAsync(TimeSpan.FromSeconds(10)); Assert.NotNull(dead);
        Assert.Equal("MaxDeliveryCountExceeded", dead.DeadLetterReason); await dlq.CompleteMessageAsync(dead);
    }
    [Fact, Trait("Category", "Broker")]
    public async Task RenewedLockSurvivesOriginalExpiry()
    {
        await using var client = Client(); await Purge(client, "relay-tests");
        await using var sender = client.CreateSender("relay-tests");
        await sender.SendMessageAsync(new("{}") { MessageId = Guid.NewGuid().ToString() });
        await using var receiver = client.CreateReceiver("relay-tests");
        var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10)); Assert.NotNull(message);
        var original = message.LockedUntil;
        await Task.Delay(TimeSpan.FromSeconds(3)); await receiver.RenewMessageLockAsync(message);
        await Task.Delay(TimeSpan.FromSeconds(3)); Assert.True(DateTimeOffset.UtcNow > original);
        await receiver.CompleteMessageAsync(message);
    }
    [Fact, Trait("Category", "Broker")]
    public async Task ExpirationDeadLettersInsteadOfDropping()
    {
        await using var client = Client(); await Purge(client, "relay-expiry");
        await using var sender = client.CreateSender("relay-expiry");
        await sender.SendMessageAsync(new("{}") { MessageId = Guid.NewGuid().ToString(), TimeToLive = TimeSpan.FromSeconds(2) });
        await Task.Delay(TimeSpan.FromSeconds(3));
        await using var receiver = client.CreateReceiver("relay-expiry");
        Assert.Null(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));
        await using var dlq = client.CreateReceiver("relay-expiry", new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        var message = await dlq.ReceiveMessageAsync(TimeSpan.FromSeconds(10)); Assert.NotNull(message);
        Assert.Equal("TTLExpiredException", message.DeadLetterReason); await dlq.CompleteMessageAsync(message);
    }
    [Fact, Trait("Category", "Broker")]
    public async Task SizeAwarePublisherSplitsBatchesAndPreservesTraceContext()
    {
        var spans = new List<System.Diagnostics.Activity>();
        using var listener = new System.Diagnostics.ActivityListener
        {
            ShouldListenTo = source => source.Name == Telemetry.Name,
            Sample = (ref System.Diagnostics.ActivityCreationOptions<System.Diagnostics.ActivityContext> _) => System.Diagnostics.ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = span => spans.Add(span)
        };
        System.Diagnostics.ActivitySource.AddActivityListener(listener);
        await using var client = Client(); await Purge(client, "relay-tests");
        await using var publisher = new ServiceBusPublisher(client, Options.Create(new RelayOptions { Queue = "relay-tests" }));
        var id = DateTime.UtcNow.Ticks;
        const string parent = "00-11111111111111111111111111111111-2222222222222222-01";
        var items = Enumerable.Range(0, 20).Select(i => new DispatchItem(id + i, "p", $"batch-{i}", new string('x', 20000), parent, 0)).ToArray();
        var outcome = await publisher.PublishAsync(items, default);
        Assert.Equal(20, outcome.Sent.Count); Assert.Empty(outcome.Quarantined);
        await using var receiver = client.CreateReceiver("relay-tests");
        var seen = new HashSet<string>();
        for (var i = 0; i < 20; i++)
        {
            var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10)); Assert.NotNull(message);
            var trace = Telemetry.Parent((string)message.ApplicationProperties["Relay-Traceparent"]);
            Assert.Equal(Telemetry.Parent(parent).TraceId, trace.TraceId);
            Assert.NotEqual(Telemetry.Parent(parent).SpanId, trace.SpanId);
            seen.Add(message.MessageId); await receiver.CompleteMessageAsync(message);
        }
        Assert.Equal(20, seen.Count);
        Assert.Equal(20, spans.Count);
        Assert.All(spans, span => Assert.Equal(Telemetry.Parent(parent).SpanId, span.ParentSpanId));
    }
    [Fact, Trait("Category", "Broker")]
    public async Task ProcessorCanStopAndRestartOutsideCallbacks()
    {
        await using var client = Client(); await Purge(client, "relay-tests");
        await using var processor = client.CreateProcessor("relay-tests", new ServiceBusProcessorOptions { AutoCompleteMessages = false, MaxConcurrentCalls = 1 });
        var received = new SemaphoreSlim(0);
        processor.ProcessMessageAsync += async args => { await args.CompleteMessageAsync(args.Message); received.Release(); };
        processor.ProcessErrorAsync += _ => Task.CompletedTask;
        await using var sender = client.CreateSender("relay-tests");
        for (var cycle = 0; cycle < 2; cycle++)
        {
            await processor.StartProcessingAsync();
            await sender.SendMessageAsync(new("{}") { MessageId = Guid.NewGuid().ToString() });
            Assert.True(await received.WaitAsync(TimeSpan.FromSeconds(10)));
            await processor.StopProcessingAsync(); Assert.False(processor.IsProcessing);
        }
        received.Dispose();
    }
}
