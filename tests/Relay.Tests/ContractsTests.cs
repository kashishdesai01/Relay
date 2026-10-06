using System.Text;
using Relay.Contracts;
using Xunit;

namespace Relay.Tests;

public sealed class ContractsTests
{
    private static readonly DateTimeOffset Now = DateTimeOffset.Parse("2026-10-13T10:00:00Z");
    internal static byte[] Event(string id = "event-1", string data = "{\"name\":\"latency\",\"value\":12,\"unit\":\"ms\"}", int version = 2) =>
        Encoding.UTF8.GetBytes($$"""{"eventId":"{{id}}","type":"metric.reported","schemaVersion":{{version}},"occurredAt":"2026-10-13T09:00:00Z","data":{{data}}} """);

    [Fact, Trait("Category", "Unit")]
    public void CanonicalHashIgnoresWhitespaceOrderAndEquivalentTypedNumbers()
    {
        var first = ContractRegistry.Validate(Event(), Now);
        var reordered = Encoding.UTF8.GetBytes("""
            { "data": { "unit":"ms", "value":12.0,"name":"latency" },
              "occurredAt":"2026-10-13T09:00:00+00:00","schemaVersion":2,"type":"metric.reported","eventId":"event-1" }
            """);
        Assert.Equal(first.Hash, ContractRegistry.Validate(reordered, Now).Hash);
    }
    [Fact, Trait("Category", "Unit")]
    public void CanonicalWireFormatIsFrozenAcrossBuilds()
    {
        var value = ContractRegistry.Validate(Event(), Now);
        Assert.Equal("""{"eventId":"event-1","type":"metric.reported","schemaVersion":2,"occurredAt":"2026-10-13T09:00:00+00:00","data":{"name":"latency","value":12,"unit":"ms"}}""", value.CanonicalJson);
    }
    [Fact, Trait("Category", "Unit")]
    public void UpcasterPreservesOriginalWireVersionForHashing()
    {
        var value = ContractRegistry.Validate(Event(data: "{\"name\":\"latency\",\"value\":12}", version: 1), Now);
        Assert.Equal("unspecified", value.Metric!.Unit);
        Assert.Equal(1, value.Envelope.SchemaVersion);
        Assert.DoesNotContain("unit", value.CanonicalJson);
    }
    [Theory, Trait("Category", "Unit")]
    [InlineData("{\"name\":null,\"value\":1,\"unit\":\"ms\"}")]
    [InlineData("{\"name\":\"x\",\"value\":1}")]
    [InlineData("{\"name\":\"x\",\"value\":1e309,\"unit\":\"ms\"}")]
    [InlineData("{\"name\":\"x\",\"value\":1,\"unit\":\"invalid\"}")]
    [InlineData("{\"name\":\"x\",\"value\":1,\"unit\":\"ms\",\"extra\":1}")]
    [InlineData("{\"name\":\"x\",\"value\":1,\"value\":2,\"unit\":\"ms\"}")]
    public void RejectsInvalidContracts(string data) => Assert.Throws<ContractException>(() => ContractRegistry.Validate(Event(data: data), Now));
    [Fact, Trait("Category", "Unit")]
    public void RejectsFutureTimestampAndUnsupportedVersion()
    {
        Assert.Throws<ContractException>(() => ContractRegistry.Validate(Event(), Now.AddDays(-1)));
        Assert.Equal("unsupported_contract", Assert.Throws<ContractException>(() => ContractRegistry.Validate(Event(version: 3), Now)).Code);
    }
    [Fact, Trait("Category", "Unit")]
    public void AcceptsMaxLengthBusinessIdWithoutUsingItAsTransportId()
    {
        Assert.Equal(128, ContractRegistry.Validate(Event(new string('x', 128)), Now).Envelope.EventId.Length);
        Assert.Equal("relay:42", new Relay.Core.DispatchItem(42, "producer", new string('x', 128), "", null, 0).TransportId);
    }
    [Fact, Trait("Category", "Unit")]
    public void RejectsOversizedBody() => Assert.Equal(413,
        Assert.Throws<ContractException>(() => ContractRegistry.Validate(new byte[ContractRegistry.MaximumBodyBytes + 1], Now)).Status);
    [Fact, Trait("Category", "Unit")]
    public void RoleParsingUsesOneWhitespaceTolerantRepresentation() =>
        Assert.Equal(["dispatcher", "consumer"], new Relay.Infrastructure.RelayOptions { Roles = "dispatcher, consumer" }.ParsedRoles);
    [Theory, Trait("Category", "Unit")]
    [InlineData("dispatcher", false)]
    [InlineData("retention", false)]
    [InlineData("dispatcher, consumer", true)]
    [InlineData("dlq-capture", true)]
    public void ReadinessSelectsOperationsAllowedByTheWorkerRole(string roles, bool receiver) =>
        Assert.Equal(receiver, new Relay.Infrastructure.RelayOptions { Roles = roles }.ReadinessUsesReceiver);
    [Fact, Trait("Category", "Unit")]
    public void RejectsOversizedOrInvalidCapturedTraceContext()
    {
        Assert.Null(Relay.Infrastructure.ServiceBusTransport.TraceParent(new string('a', 32000)));
        Assert.Null(Relay.Infrastructure.ServiceBusTransport.TraceParent("invalid"));
        const string valid = "00-11111111111111111111111111111111-2222222222222222-01";
        Assert.Equal(valid, Relay.Infrastructure.ServiceBusTransport.TraceParent(valid));
    }
}
