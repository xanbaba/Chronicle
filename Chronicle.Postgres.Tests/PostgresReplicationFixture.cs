using Testcontainers.PostgreSql;

namespace Chronicle.Postgres.Tests;

public class PostgresReplicationFixture : IAsyncLifetime
{
    public readonly PostgreSqlContainer Container = new PostgreSqlBuilder("postgres:18-alpine")
        .WithCommand("-c", "wal_level=logical")
        .Build();
    
    public static string PublicationName => "chronicle_pub";

    public async Task InitializeAsync()
    {
        await Container.StartAsync();
        
        var result = await Container.ExecScriptAsync("ALTER ROLE postgres REPLICATION LOGIN;");
        if (result.ExitCode != 0)
            throw new InvalidOperationException($"Failed to set replication role: {result.Stderr}");
        
        result = await Container.ExecScriptAsync($"CREATE PUBLICATION {PublicationName} FOR ALL TABLES;");
        if (result.ExitCode != 0)
            throw new InvalidOperationException($"Failed to create publication: {result.Stderr}");
    }

    public async Task DisposeAsync()
    {
        await Container.DisposeAsync();
    }
}