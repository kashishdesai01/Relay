using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace Relay.Contracts;

public sealed record EventEnvelope(string EventId, string Type, int SchemaVersion, DateTimeOffset OccurredAt, JsonElement Data);
public sealed record MetricV1(string Name, double Value);
public sealed record MetricV2(string Name, double Value, string Unit);
public sealed record DeploymentCompleted(string Service, string Version, bool Success);
public sealed record LogBatch(string Severity, string[] Entries);
public sealed record ValidatedEvent(EventEnvelope Envelope, string CanonicalJson, byte[] Hash, MetricV2? Metric);
public sealed class ContractException(int status, string code) : Exception(code)
{
    public int Status { get; } = status;
    public string Code { get; } = code;
}

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow, RespectNullableAnnotations = true)]
[JsonSerializable(typeof(EventEnvelope))]
[JsonSerializable(typeof(MetricV1))]
[JsonSerializable(typeof(MetricV2))]
[JsonSerializable(typeof(DeploymentCompleted))]
[JsonSerializable(typeof(LogBatch))]
public partial class EventJson : JsonSerializerContext;

public static partial class ContractRegistry
{
    public const int MaximumBodyBytes = 200 * 1024;
    [GeneratedRegex("^[A-Za-z0-9._-]{1,128}$", RegexOptions.CultureInvariant)]
    private static partial Regex Identifier();

    public static ValidatedEvent Validate(ReadOnlyMemory<byte> body, DateTimeOffset now)
    {
        if (body.Length > MaximumBodyBytes) throw new ContractException(413, "body_too_large");
        try
        {
            using var document = JsonDocument.Parse(body, new JsonDocumentOptions { MaxDepth = 16 });
            RejectDuplicateProperties(document.RootElement);
            var root = document.RootElement;
            RequireFields(root, "eventId", "type", "schemaVersion", "occurredAt", "data");
            var envelope = root.Deserialize(EventJson.Default.EventEnvelope) ?? throw Invalid();
            if (!Identifier().IsMatch(envelope.EventId) || envelope.OccurredAt == default ||
                envelope.OccurredAt > now.AddMinutes(5)) throw Invalid();
            MetricV2? metric = null;
            JsonElement canonicalData;
            switch (envelope.Type, envelope.SchemaVersion)
            {
                case ("metric.reported", 1):
                    RequireFields(envelope.Data, "name", "value");
                    var v1 = envelope.Data.Deserialize(EventJson.Default.MetricV1) ?? throw Invalid();
                    metric = new(v1.Name, v1.Value, "unspecified");
                    ValidateMetric(metric);
                    canonicalData = JsonSerializer.SerializeToElement(v1, EventJson.Default.MetricV1);
                    break;
                case ("metric.reported", 2):
                    RequireFields(envelope.Data, "name", "value", "unit");
                    metric = envelope.Data.Deserialize(EventJson.Default.MetricV2) ?? throw Invalid();
                    ValidateMetric(metric);
                    canonicalData = JsonSerializer.SerializeToElement(metric, EventJson.Default.MetricV2);
                    break;
                case ("deployment.completed", 1):
                    RequireFields(envelope.Data, "service", "version", "success");
                    var deployment = envelope.Data.Deserialize(EventJson.Default.DeploymentCompleted) ?? throw Invalid();
                    if (!Identifier().IsMatch(deployment.Service) || deployment.Version.Length is < 1 or > 128) throw Invalid();
                    canonicalData = JsonSerializer.SerializeToElement(deployment, EventJson.Default.DeploymentCompleted);
                    break;
                case ("log.batch", 1):
                    RequireFields(envelope.Data, "severity", "entries");
                    var logs = envelope.Data.Deserialize(EventJson.Default.LogBatch) ?? throw Invalid();
                    if (logs.Severity is not ("info" or "warning" or "error") || logs.Entries.Length is < 1 or > 100 ||
                        logs.Entries.Any(e => e is null || e.Length > 1024)) throw Invalid();
                    canonicalData = JsonSerializer.SerializeToElement(logs, EventJson.Default.LogBatch);
                    break;
                default: throw new ContractException(422, "unsupported_contract");
            }
            var canonical = JsonSerializer.SerializeToUtf8Bytes(envelope with
            {
                OccurredAt = envelope.OccurredAt.ToUniversalTime(),
                Data = canonicalData
            }, EventJson.Default.EventEnvelope);
            if (canonical.Length > MaximumBodyBytes) throw new ContractException(413, "canonical_body_too_large");
            return new(envelope with { Data = canonicalData }, System.Text.Encoding.UTF8.GetString(canonical),
                SHA256.HashData(canonical), metric);
        }
        catch (JsonException) { throw new ContractException(400, "malformed_json"); }
        catch (InvalidOperationException) { throw new ContractException(400, "malformed_json"); }
        catch (ArgumentException) { throw new ContractException(422, "invalid_contract"); }
    }

    private static ContractException Invalid() => new(422, "invalid_contract");
    private static void ValidateMetric(MetricV2 metric)
    {
        if (!Identifier().IsMatch(metric.Name) || !double.IsFinite(metric.Value) || Math.Abs(metric.Value) > 1e12 ||
            metric.Unit is not ("unspecified" or "count" or "ms" or "seconds" or "bytes" or "percent")) throw Invalid();
    }
    private static void RequireFields(JsonElement value, params string[] fields)
    {
        if (value.ValueKind != JsonValueKind.Object || fields.Any(f => !value.TryGetProperty(f, out var v) || v.ValueKind == JsonValueKind.Null))
            throw Invalid();
    }
    private static void RejectDuplicateProperties(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.Object)
        {
            var names = new HashSet<string>(StringComparer.Ordinal);
            foreach (var property in element.EnumerateObject())
            {
                if (!names.Add(property.Name)) throw new ContractException(400, "duplicate_property");
                RejectDuplicateProperties(property.Value);
            }
        }
        else if (element.ValueKind == JsonValueKind.Array)
            foreach (var child in element.EnumerateArray()) RejectDuplicateProperties(child);
    }
}
