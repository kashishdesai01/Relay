using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Options;
using Relay.Core;

namespace Relay.Api;

public sealed class ApiKeyAuthentication(IOptionsMonitor<AuthenticationSchemeOptions> options, ILoggerFactory logger,
    UrlEncoder encoder, IRelayStore store) : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        if (Context.GetEndpoint()?.Metadata.GetOrderedMetadata<IAuthorizeData>().Count is not > 0)
            return AuthenticateResult.NoResult();
        if (!Request.Headers.TryGetValue("X-Relay-Key", out var supplied)) return AuthenticateResult.NoResult();
        var key = supplied.ToString();
        if (key.Length != 64 || key.Any(c => !char.IsAsciiHexDigit(c))) return AuthenticateResult.Fail("Invalid credentials");
        var producer = await store.AuthenticateAsync(SHA256.HashData(Encoding.UTF8.GetBytes(key)), Context.RequestAborted);
        if (producer is null) return AuthenticateResult.Fail("Invalid credentials");
        var claims = new List<Claim> { new(ClaimTypes.NameIdentifier, producer.Id) };
        claims.AddRange(producer.Scopes.Select(scope => new Claim("scope", scope)));
        return AuthenticateResult.Success(new(new(new ClaimsIdentity(claims, Scheme.Name)), Scheme.Name));
    }
    protected override Task HandleChallengeAsync(AuthenticationProperties properties) =>
        Results.Problem(statusCode: 401, title: "Authentication required", type: "urn:relay:unauthorized").ExecuteAsync(Context);
    protected override Task HandleForbiddenAsync(AuthenticationProperties properties) =>
        Results.Problem(statusCode: 403, title: "Scope required", type: "urn:relay:forbidden").ExecuteAsync(Context);
}
