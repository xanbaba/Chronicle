using System.Globalization;
using Chronicle.Abstractions;

namespace Chronicle.Postgres.Tests;

public class PostgresReplicationSourceTests(PostgresReplicationFixture fixture)
    : IClassFixture<PostgresReplicationFixture>
{
    [Fact]
    public async Task Insert_ShouldProduceInsertChangeEventWithCorrectFields()
    {
        // arrange — create slot, table, set up source
        var tableName = $"test_table_{Guid.NewGuid():N}";
        var slotName = $"test_slot_{Guid.NewGuid():N}";

        try
        {
            await fixture.Container.ExecScriptAsync(
                $"CREATE TABLE {tableName} (id int PRIMARY KEY, first_name VARCHAR(100), created_at TIMESTAMP);");

            await fixture.Container.ExecScriptAsync(
                $"SELECT pg_create_logical_replication_slot('{slotName}', 'pgoutput');");

            // act — insert row, read from StreamAsync
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

            var streamingConnectionString = fixture.Container.GetConnectionString();

            var replicationOptions =
                new PostgresReplicationOptions(streamingConnectionString, slotName,
                    PostgresReplicationFixture.PublicationName);

            var insertCreatedAt = DateTime.UtcNow.TruncateToSeconds();

            var streamingTask = StreamAndAssertAsync(replicationOptions, AssertAction, cts.Token);
            
            var execResult =
                await fixture.Container.ExecScriptAsync(
                    $"INSERT INTO {tableName} VALUES (1, 'John', '{insertCreatedAt:O}');");

            if (execResult.ExitCode != 0)
            {
                Assert.Fail($"Failed to insert row: {execResult.Stderr}");
            }

            try
            {
                await streamingTask;
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested)
            {
                // timeout — test should fail here
                Assert.Fail("No change event received within timeout");
            }

            void AssertAction(RawChangeEvent rawChangeEvent)
            {
                Assert.Equal(ChangeOperation.Insert, rawChangeEvent.Operation);
                Assert.Equal(tableName, rawChangeEvent.TableName);
                Assert.Equal(1, Convert.ToInt32(rawChangeEvent.After["id"]));
                Assert.Equal("John", rawChangeEvent.After["first_name"]);

                if (rawChangeEvent.After["created_at"] is null)
                {
                    Assert.Fail("created_at is null");
                }

                Assert.IsType<string>(rawChangeEvent.After["created_at"]);

                var actualCreatedAt = DateTime.SpecifyKind(DateTime.ParseExact(rawChangeEvent.After["created_at"]!.ToString()!, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture), DateTimeKind.Utc);

                Assert.Equal(insertCreatedAt, actualCreatedAt);
                Assert.Empty(rawChangeEvent.Before);
                Assert.IsType<PostgresReplicationOffset>(rawChangeEvent.Offset);
                Assert.Equal("public", rawChangeEvent.SchemaName);
            }
        }
        finally
        {
            // clean up
            await fixture.Container.ExecScriptAsync($"SELECT pg_drop_replication_slot('{slotName}');");
            await fixture.Container.ExecScriptAsync($"DROP TABLE {tableName};");
        }
    }

    private async Task StreamAndAssertAsync(
        PostgresReplicationOptions replicationOptions,
        Action<RawChangeEvent> assert,
        CancellationToken cancellationToken)
    {
        var replicationSource = new PostgresReplicationSource();

        await foreach (var rawChangeEvent in replicationSource.StreamAsync(replicationOptions, cancellationToken))
        {
            try
            {
                // assert — verify RawChangeEvent fields
                assert(rawChangeEvent);

                return;
            }
            finally
            {
                // clean up
                await replicationSource.ConfirmAsync(rawChangeEvent.Offset, cancellationToken);
            }
        }

        Assert.Fail("Stream completed without yielding a change event.");
    }
}