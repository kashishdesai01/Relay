using System.Security.Cryptography;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Relay.Infrastructure;
using Relay.Infrastructure.Migrations;
using Xunit;

namespace Relay.Tests;

[CollectionDefinition("Database", DisableParallelization = true)]
public sealed class DatabaseCollection : ICollectionFixture<PostgresFixture>;

public sealed class PostgresFixture : IAsyncLifetime
{
    public NpgsqlDataSource Source { get; private set; } = null!;
    public PostgresStore Store { get; private set; } = null!;
    public async Task InitializeAsync()
    {
        var connectionString = Environment.GetEnvironmentVariable("RELAY_TEST_DATABASE") ?? throw new InvalidOperationException(
            "RELAY_TEST_DATABASE must point to an isolated relay_test database. Use scripts/test.sh.");
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        if (builder.Database is null || !builder.Database.StartsWith("relay_test", StringComparison.Ordinal))
            throw new InvalidOperationException("Tests only allow a database named relay_test or relay_test_*.");
        Source = Runtime.DataSource(connectionString, "Relay.Tests");
        var options = new DbContextOptionsBuilder<RelayDbContext>().UseNpgsql(connectionString).Options;
        await using var db = new RelayDbContext(options);
        await db.Database.MigrateAsync();
        Store = new(Source, TimeProvider.System);
    }
    public async Task Reset()
    {
        await using var connection = await Source.OpenConnectionAsync();
        await using var command = new NpgsqlCommand("""
            TRUNCATE incident_actions,incidents,capture_receipts,rollups_minute,event_records,
              processed_events,outbox,event_payloads,events,producers RESTART IDENTITY CASCADE;
            INSERT INTO producers(id,name,key_hash,scopes) VALUES('p','producer',@hash,ARRAY['events.read','events.write','relay.admin']);
            """, connection);
        command.Parameters.AddWithValue("hash", SHA256.HashData(Encoding.UTF8.GetBytes(new string('a', 64))));
        await command.ExecuteNonQueryAsync();
    }
    public async Task<long> Count(string table)
    {
        // Names are test literals, never user input.
        if (!new[] { "events", "outbox", "processed_events", "event_records", "incidents", "incident_actions", "event_payloads", "capture_receipts" }.Contains(table)) throw new ArgumentException("Invalid test table");
        await using var connection = await Source.OpenConnectionAsync();
        await using var command = new NpgsqlCommand($"SELECT count(*) FROM {table}", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }
    public async Task Execute(string sql)
    {
        await using var connection = await Source.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }
    public async Task DisposeAsync() => await Source.DisposeAsync();
}
