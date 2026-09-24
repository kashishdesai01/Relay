using System.Security.Cryptography;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Relay.Infrastructure.Migrations;

var database = Environment.GetEnvironmentVariable("Relay__Database") ?? throw new InvalidOperationException("Relay__Database is required");
await using var connection = new NpgsqlConnection(database);
await connection.OpenAsync();
await using (var guard = new NpgsqlCommand("SELECT pg_advisory_lock(72813421)", connection)) await guard.ExecuteNonQueryAsync();
try
{
    var options = new DbContextOptionsBuilder<RelayDbContext>().UseNpgsql(connection).Options;
    await using var context = new RelayDbContext(options);
    await context.Database.MigrateAsync();
    if (args.Length == 3 && args[0] == "provision")
    {
        var producerId = args[1];
        var scopes = args[2].Split(',');
        if (producerId.Length is < 1 or > 128 || producerId.Any(c => !char.IsAsciiLetterOrDigit(c) && c is not ('.' or '_' or '-')) ||
            scopes.Any(s => s is not ("events.write" or "events.read" or "relay.admin"))) throw new ArgumentException("Invalid producer or scope");
        var key = Environment.GetEnvironmentVariable("RELAY_PROVISION_KEY") ?? Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(32));
        if (key.Length != 64 || key.Any(c => !char.IsAsciiHexDigit(c))) throw new ArgumentException("Provision key must be 64 hex characters");
        await using var command = new NpgsqlCommand("INSERT INTO producers(id,name,key_hash,scopes) VALUES(@id,@id,@hash,@scopes) ON CONFLICT(id) DO NOTHING", connection);
        command.Parameters.AddWithValue("id", producerId);
        command.Parameters.AddWithValue("hash", SHA256.HashData(Encoding.UTF8.GetBytes(key)));
        command.Parameters.AddWithValue("scopes", scopes);
        var inserted = await command.ExecuteNonQueryAsync();
        if (inserted == 0)
        {
            await using var verify = new NpgsqlCommand("SELECT key_hash,scopes,disabled FROM producers WHERE id=@id", connection);
            verify.Parameters.AddWithValue("id", producerId);
            await using var reader = await verify.ExecuteReaderAsync();
            await reader.ReadAsync();
            if (!CryptographicOperations.FixedTimeEquals(reader.GetFieldValue<byte[]>(0), SHA256.HashData(Encoding.UTF8.GetBytes(key))) ||
                !reader.GetFieldValue<string[]>(1).Order().SequenceEqual(scopes.Order()) || reader.GetBoolean(2))
                throw new InvalidOperationException("Existing producer has different credentials/scopes or is disabled; use the documented rotation/recovery procedure.");
        }
        // This is the explicit one-time provisioning output; normal service logs never contain keys.
        Console.WriteLine(key);
    }
}
finally
{
    await using var unlock = new NpgsqlCommand("SELECT pg_advisory_unlock(72813421)", connection);
    await unlock.ExecuteNonQueryAsync();
}
