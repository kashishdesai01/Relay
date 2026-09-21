using System.Collections.Concurrent;
using System.Diagnostics;

namespace Relay.Infrastructure;

public static class Failpoints
{
#if FAILPOINTS
    public static bool IsEnabled(string name) => Environment.GetEnvironmentVariable("RELAY_FAILPOINTS") == name;
    public static async Task PauseAsync(CancellationToken ct)
    {
        if (!IsEnabled("consumer.pause")) return;
        var marker = Environment.GetEnvironmentVariable("RELAY_PAUSE_MARKER") ?? throw new InvalidOperationException("Pause marker is required");
        await File.WriteAllTextAsync(marker, "entered", ct);
        await Task.Delay(TimeSpan.FromSeconds(2), ct);
    }
#endif
    [Conditional("FAILPOINTS")]
    public static void Hit(string name)
    {
#if FAILPOINTS
        var configured = Environment.GetEnvironmentVariable("RELAY_FAILPOINTS");
        if (configured != name) return;
        if (name == "consumer.permanent_failure") throw new Relay.Core.PermanentDeliveryException("injected_handler_failure");
        Process.GetCurrentProcess().Kill();
#endif
    }
}
