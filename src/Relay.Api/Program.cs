using System.Diagnostics;
using System.Security.Claims;
using System.Threading.RateLimiting;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.Extensions.Options;
using Relay.Api;
using Relay.Contracts;
using Relay.Core;
using Relay.Infrastructure;

var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole();
builder.WebHost.ConfigureKestrel(o => o.Limits.MaxRequestBodySize = ContractRegistry.MaximumBodyBytes);
builder.Services.AddRelay(builder.Configuration, "Relay.Api");
builder.Services.AddProblemDetails();
builder.Services.AddSingleton<AuthenticationGate>();
builder.Services.AddAuthentication("ApiKey").AddScheme<AuthenticationSchemeOptions, ApiKeyAuthentication>("ApiKey", _ => { });
builder.Services.AddAuthorization(o =>
{
    o.AddPolicy("write", p => p.RequireAuthenticatedUser().RequireClaim("scope", "events.write"));
    o.AddPolicy("read", p => p.RequireAuthenticatedUser().RequireClaim("scope", "events.read"));
    o.AddPolicy("admin", p => p.RequireAuthenticatedUser().RequireClaim("scope", "relay.admin"));
});
builder.Services.AddRateLimiter(o =>
{
    o.GlobalLimiter = PartitionedRateLimiter.Create<HttpContext, string>(context => context.User.Identity?.IsAuthenticated == true
        ? RateLimitPartition.GetNoLimiter("authenticated")
        : RateLimitPartition.GetTokenBucketLimiter(context.Connection.RemoteIpAddress?.ToString() ?? "unknown", _ => new()
        { TokenLimit = 20, TokensPerPeriod = 5, ReplenishmentPeriod = TimeSpan.FromSeconds(1), AutoReplenishment = true, QueueLimit = 0 }));
    o.AddPolicy("producer", context => RateLimitPartition.GetTokenBucketLimiter(
        context.User.FindFirstValue(ClaimTypes.NameIdentifier) ?? "anonymous", _ =>
        {
            var limit = context.RequestServices.GetRequiredService<IOptions<RelayOptions>>().Value.ProducerRequestsPerSecond;
            return new() { TokenLimit = limit, TokensPerPeriod = limit, ReplenishmentPeriod = TimeSpan.FromSeconds(1), AutoReplenishment = true, QueueLimit = 0 };
        }));
    o.OnRejected = async (context, token) =>
    {
        context.HttpContext.Response.Headers.RetryAfter = "1";
        await Results.Problem(statusCode: 429, title: "Request rate exceeded", type: "urn:relay:rate_limited").ExecuteAsync(context.HttpContext);
    };
});
builder.Services.AddSingleton<BacklogMonitor>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<BacklogMonitor>());
var app = builder.Build();
app.Use(async (context, next) =>
{
    context.Response.Headers.ContentSecurityPolicy = "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'";
    context.Response.Headers.XContentTypeOptions = "nosniff";
    context.Response.Headers.CacheControl = "no-store";
    try { await next(context); }
    catch (ContractException e) { await Problem(context, e.Status, e.Code); }
    catch (StateConflictException e) { await Problem(context, 409, e.Message); }
    catch (BadHttpRequestException e) { await Problem(context, e.StatusCode, e.StatusCode == 413 ? "body_too_large" : "bad_request"); }
    catch (Exception e) when (!context.RequestAborted.IsCancellationRequested)
    {
        app.Logger.LogError("Request failed; category {Category}; trace {Trace}", e.GetType().Name, Activity.Current?.TraceId);
        context.Response.Headers.RetryAfter = "1";
        await Problem(context, 503, "dependency_unavailable");
    }
});
app.UseDefaultFiles();
app.UseStaticFiles();
app.UseRouting();
app.Use(async (context, next) =>
{
    if (context.GetEndpoint()?.Metadata.GetOrderedMetadata<Microsoft.AspNetCore.Authorization.IAuthorizeData>().Count is > 0)
    {
        using var lease = context.RequestServices.GetRequiredService<AuthenticationGate>().Acquire(context);
        if (!lease.IsAcquired)
        {
            context.Response.Headers.RetryAfter = "1";
            await Problem(context, 429, "authentication_budget_exceeded");
            return;
        }
    }
    await next(context);
});
app.UseAuthentication();
app.UseRateLimiter();
app.UseAuthorization();
app.MapGet("/health/live", () => Results.Ok(new { status = "alive" })).DisableRateLimiting();
app.MapGet("/health/ready", async (IRelayStore store, CancellationToken ct) =>
    await store.ProbeAsync(ct) ? Results.Ok(new { status = "ready" }) : Results.StatusCode(503)).DisableRateLimiting();
app.MapPost("/v1/events", async (HttpContext context, IRelayStore store, BacklogMonitor monitor, IOptions<RelayOptions> options, CancellationToken ct) =>
{
    var snapshot = monitor.Current;
    if (snapshot.Backlog.Count >= options.Value.BacklogLimit || snapshot.Backlog.OldestSeconds > options.Value.OldestPendingLimitSeconds ||
        DateTimeOffset.UtcNow - snapshot.At > TimeSpan.FromSeconds(10))
    {
        context.Response.Headers.RetryAfter = "1";
        return Results.Problem(statusCode: 503, title: "Admission temporarily paused", type: "urn:relay:backlog_shedding");
    }
    var started = Stopwatch.GetTimestamp();
    using var span = Telemetry.Activities.StartActivity("ingest.commit", ActivityKind.Internal);
    using var stream = new MemoryStream();
    await context.Request.Body.CopyToAsync(stream, ct);
    var value = ContractRegistry.Validate(stream.ToArray(), DateTimeOffset.UtcNow);
    var producer = context.User.FindFirstValue(ClaimTypes.NameIdentifier)!;
    var admission = await store.AdmitAsync(producer, value, span?.Id ?? Activity.Current?.Id, ct);
    Telemetry.IngestDuration.Record(Stopwatch.GetElapsedTime(started).TotalSeconds);
    if (admission.Conflict) { Telemetry.Rejected.Add(1, new KeyValuePair<string, object?>("reason", "identity_conflict")); return Results.Problem(statusCode: 409, title: "Event ID already has different content", type: "urn:relay:identity_conflict"); }
    if (admission.Replay) context.Response.Headers["Relay-Idempotent-Replay"] = "true";
    else Telemetry.Accepted.Add(1, new KeyValuePair<string, object?>("type", value.Envelope.Type));
    return Results.Accepted($"/v1/events/{value.Envelope.EventId}", new { eventId = value.Envelope.EventId, status = "accepted" });
}).RequireAuthorization("write").RequireRateLimiting("producer");
app.MapGet("/v1/events/{eventId}", async (string eventId, HttpContext context, IRelayStore store, CancellationToken ct) =>
{
    var result = await store.StatusAsync(context.User.FindFirstValue(ClaimTypes.NameIdentifier)!, eventId, ct);
    return result is null ? Results.NotFound() : Results.Ok(result);
}).RequireAuthorization("read");
var admin = app.MapGroup("/v1/admin").RequireAuthorization("admin").RequireRateLimiting("producer");
admin.MapGet("/incidents", async (long? before, int? limit, IRelayStore store, CancellationToken ct) =>
    Results.Ok(await store.IncidentsAsync(before ?? long.MaxValue, Math.Clamp(limit ?? 50, 1, 100), ct)));
admin.MapGet("/incidents/{id:long}", async (long id, IRelayStore store, CancellationToken ct) =>
{
    var incident = await store.IncidentAsync(id, ct);
    return incident is null ? Results.NotFound() : Results.Ok(incident);
});
admin.MapPost("/incidents/{id:long}/{action}", async (long id, string action, HttpContext context, IRelayStore store, CancellationToken ct) =>
{
    var result = await store.ResolveAsync(id, action, context.User.FindFirstValue(ClaimTypes.NameIdentifier)!, ct);
    return result is null ? Results.NotFound() : Results.Ok(result);
});
await app.RunAsync();

static Task Problem(HttpContext context, int status, string code) => Results.Problem(
    statusCode: status, title: code.Replace('_', ' '), type: $"urn:relay:{code}",
    extensions: new Dictionary<string, object?> { ["traceId"] = Activity.Current?.TraceId.ToString() }).ExecuteAsync(context);
