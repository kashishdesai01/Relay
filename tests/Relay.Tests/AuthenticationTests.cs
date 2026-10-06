using System.Net;
using System.Reflection;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Relay.Api;
using Relay.Core;
using Xunit;

namespace Relay.Tests;

public sealed class AuthenticationTests
{
    [Theory, Trait("Category", "Unit")]
    [InlineData("/health/live")]
    [InlineData("/health/ready")]
    [InlineData("/unmatched")]
    public async Task PublicAndUnmatchedPathsNeverLookUpSuppliedCredentials(string path)
    {
        var store = DispatchProxy.Create<IRelayStore, LookupProbe>();
        var context = new DefaultHttpContext(); context.Request.Path = path;
        context.Request.Headers["X-Relay-Key"] = new string('a', 64);
        context.SetEndpoint(new Endpoint(_ => Task.CompletedTask, new EndpointMetadataCollection(), "public"));
        var handler = new ApiKeyAuthentication(new Monitor(), NullLoggerFactory.Instance, UrlEncoder.Default, store);
        await handler.InitializeAsync(new AuthenticationScheme("ApiKey", null, typeof(ApiKeyAuthentication)), context);
        Assert.True((await handler.AuthenticateAsync()).None);
        Assert.Equal(0, ((LookupProbe)(object)store).Lookups);
    }
    [Fact, Trait("Category", "Unit")]
    public async Task ProtectedEndpointAuthenticatesAgainstStore()
    {
        var store = DispatchProxy.Create<IRelayStore, LookupProbe>();
        var context = new DefaultHttpContext(); context.Request.Path = "/v1/events";
        context.Request.Headers["X-Relay-Key"] = new string('a', 64);
        context.SetEndpoint(new Endpoint(_ => Task.CompletedTask, new EndpointMetadataCollection(new AuthorizeAttribute("write")), "protected"));
        var handler = new ApiKeyAuthentication(new Monitor(), NullLoggerFactory.Instance, UrlEncoder.Default, store);
        await handler.InitializeAsync(new AuthenticationScheme("ApiKey", null, typeof(ApiKeyAuthentication)), context);
        Assert.NotNull((await handler.AuthenticateAsync()).Failure);
        Assert.Equal(1, ((LookupProbe)(object)store).Lookups);
    }
    [Fact, Trait("Category", "Unit")]
    public void DistinctValidShapedKeysShareThePreAuthenticationIpBudget()
    {
        using var gate = new AuthenticationGate();
        var admitted = 0;
        for (var i = 0; i < 2000; i++)
        {
            var context = new DefaultHttpContext();
            context.Connection.RemoteIpAddress = IPAddress.Loopback;
            context.Request.Headers["X-Relay-Key"] = i.ToString("x64");
            using var lease = gate.Acquire(context);
            if (lease.IsAcquired) admitted++;
        }
        Assert.Equal(1000, admitted);
    }
    public class LookupProbe : DispatchProxy
    {
        public int Lookups { get; private set; }
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod?.Name != nameof(IRelayStore.AuthenticateAsync)) throw new InvalidOperationException("Unexpected store operation");
            Lookups++;
            return Task.FromResult<Producer?>(null);
        }
    }
    private sealed class Monitor : IOptionsMonitor<AuthenticationSchemeOptions>
    {
        public AuthenticationSchemeOptions CurrentValue { get; } = new();
        public AuthenticationSchemeOptions Get(string? name) => CurrentValue;
        public IDisposable? OnChange(Action<AuthenticationSchemeOptions, string?> listener) => null;
    }
}
