using System.Threading.RateLimiting;

namespace Relay.Api;

// Apply before authentication so valid-shaped guessed keys cannot bypass the admission budget.
public sealed class AuthenticationGate : IDisposable
{
    private readonly PartitionedRateLimiter<HttpContext> _limiter = PartitionedRateLimiter.Create<HttpContext, string>(context =>
        RateLimitPartition.GetTokenBucketLimiter(context.Connection.RemoteIpAddress?.ToString() ?? "unknown", _ => new()
        {
            TokenLimit = 1000,
            TokensPerPeriod = 1000,
            ReplenishmentPeriod = TimeSpan.FromSeconds(1),
            AutoReplenishment = true,
            QueueLimit = 0
        }));
    public RateLimitLease Acquire(HttpContext context) => _limiter.AttemptAcquire(context);
    public void Dispose() => _limiter.Dispose();
}
