using System.Data;
using Npgsql;

namespace Relay.Infrastructure;

internal static class Sql
{
    public static NpgsqlCommand Command(NpgsqlConnection connection, NpgsqlTransaction? transaction, string sql,
        params (string Name, object? Value)[] parameters)
    {
        var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = 10 };
        foreach (var (name, value) in parameters)
        {
            if (value is null) command.Parameters.Add(new NpgsqlParameter(name, DbType.String) { Value = DBNull.Value });
            else command.Parameters.AddWithValue(name, value);
        }
        return command;
    }
    public static async Task<int> Execute(NpgsqlConnection connection, NpgsqlTransaction? transaction, string sql,
        CancellationToken ct, params (string Name, object? Value)[] parameters)
    {
        await using var command = Command(connection, transaction, sql, parameters);
        return await command.ExecuteNonQueryAsync(ct);
    }
    public static async Task<object?> Scalar(NpgsqlConnection connection, NpgsqlTransaction? transaction, string sql,
        CancellationToken ct, params (string Name, object? Value)[] parameters)
    {
        await using var command = Command(connection, transaction, sql, parameters);
        return await command.ExecuteScalarAsync(ct);
    }
}
