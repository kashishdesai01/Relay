using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Relay.Infrastructure.Migrations;

// EF owns versioned schema evolution; hot-path repositories use parameterized Npgsql SQL.
public sealed class RelayDbContext(DbContextOptions<RelayDbContext> options) : DbContext(options);

[DbContext(typeof(RelayDbContext))]
[Migration("202610130001_Initial")]
public sealed class Initial : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        using var stream = typeof(Initial).Assembly.GetManifestResourceStream("Relay.Infrastructure.Migrations.001_initial.sql")!;
        using var reader = new StreamReader(stream);
        migrationBuilder.Sql(reader.ReadToEnd());
    }
    protected override void Down(MigrationBuilder migrationBuilder) =>
        throw new NotSupportedException("Restore a verified backup; destructive down migrations are deliberately unsupported.");
}

[DbContext(typeof(RelayDbContext))]
[Migration("202610130002_Diagnostics")]
public sealed class Diagnostics : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        using var stream = typeof(Diagnostics).Assembly.GetManifestResourceStream("Relay.Infrastructure.Migrations.002_diagnostics.sql")!;
        using var reader = new StreamReader(stream);
        migrationBuilder.Sql(reader.ReadToEnd());
    }
    protected override void Down(MigrationBuilder migrationBuilder) =>
        throw new NotSupportedException("Restore a verified backup; destructive down migrations are deliberately unsupported.");
}
