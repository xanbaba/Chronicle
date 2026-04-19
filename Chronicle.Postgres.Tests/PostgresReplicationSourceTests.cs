using System.Globalization;
using Chronicle.Abstractions;

namespace Chronicle.Postgres.Tests;

public class PostgresReplicationSourceTests(PostgresReplicationFixture fixture)
    : IClassFixture<PostgresReplicationFixture>
{
    // ─── Insert ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task Insert_ShouldProduceInsertChangeEventWithCorrectFields()
    {
        var tableName = $"test_table_{Guid.NewGuid():N}";
        var slotName = $"test_slot_{Guid.NewGuid():N}";

        try
        {
            await fixture.Container.ExecScriptAsync(
                $"CREATE TABLE {tableName} (id int PRIMARY KEY, first_name VARCHAR(100), created_at TIMESTAMP);");
            await fixture.Container.ExecScriptAsync(
                $"SELECT pg_create_logical_replication_slot('{slotName}', 'pgoutput');");

            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var replicationOptions = new PostgresReplicationOptions(
                fixture.Container.GetConnectionString(), slotName, PostgresReplicationFixture.PublicationName);

            var insertCreatedAt = DateTime.UtcNow.TruncateToSeconds();
            
            var replicationSource = new PostgresReplicationSource();
            
            var streamingTask = StreamAndAssertAsync(replicationSource, replicationOptions, AssertAction, cts.Token);

            var execResult = await fixture.Container.ExecScriptAsync(
                $"INSERT INTO {tableName} VALUES (1, 'John', '{insertCreatedAt:O}');");
            if (execResult.ExitCode != 0)
                Assert.Fail($"Failed to insert row: {execResult.Stderr}");

            try { await streamingTask; }
            catch (OperationCanceledException) when (cts.IsCancellationRequested)
            { Assert.Fail("No change event received within timeout"); }

            void AssertAction(RawChangeEvent e)
            {
                Assert.Equal(ChangeOperation.Insert, e.Operation);
                Assert.Equal(tableName, e.TableName);
                Assert.Equal("public", e.SchemaName);
                Assert.Equal(1, Convert.ToInt32(e.After["id"]));
                Assert.Equal("John", e.After["first_name"]);

                if (e.After["created_at"] is null)
                    Assert.Fail("created_at is null");

                Assert.IsType<string>(e.After["created_at"]);

                var actualCreatedAt = DateTime.SpecifyKind(
                    DateTime.ParseExact(e.After["created_at"]!.ToString()!,
                        "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
                    DateTimeKind.Utc);

                Assert.Equal(insertCreatedAt, actualCreatedAt);
                Assert.Empty(e.Before);
                Assert.IsType<PostgresReplicationOffset>(e.Offset);
            }
        }
        finally
        {
            await fixture.Container.ExecScriptAsync($"SELECT pg_drop_replication_slot('{slotName}');");
            await fixture.Container.ExecScriptAsync($"DROP TABLE {tableName};");
        }
    }

    // ─── Update ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task Update_ShouldProduceUpdateChangeEvent_WithEmptyBefore_WhenReplicaIdentityDefault()
    {
        var tableName = $"test_table_{Guid.NewGuid():N}";
        var slotName = $"test_slot_{Guid.NewGuid():N}";

        try
        {
            await fixture.Container.ExecScriptAsync(
                $"CREATE TABLE {tableName} (id int PRIMARY KEY, first_name VARCHAR(100));");
            await fixture.Container.ExecScriptAsync(
                $"INSERT INTO {tableName} VALUES (1, 'John');");
            await fixture.Container.ExecScriptAsync(
                $"SELECT pg_create_logical_replication_slot('{slotName}', 'pgoutput');");

            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var replicationOptions = new PostgresReplicationOptions(
                fixture.Container.GetConnectionString(), slotName, PostgresReplicationFixture.PublicationName);

            var replicationSource = new PostgresReplicationSource();
            
            var streamingTask = StreamAndAssertAsync(replicationSource, replicationOptions, AssertAction, cts.Token);

            var execResult = await fixture.Container.ExecScriptAsync(
                $"UPDATE {tableName} SET first_name = 'Jane' WHERE id = 1;");
            if (execResult.ExitCode != 0)
                Assert.Fail($"Failed to update row: {execResult.Stderr}");

            try { await streamingTask; }
            catch (OperationCanceledException) when (cts.IsCancellationRequested)
            { Assert.Fail("No change event received within timeout"); }

            void AssertAction(RawChangeEvent e)
            {
                Assert.Equal(ChangeOperation.Update, e.Operation);
                Assert.Equal(tableName, e.TableName);
                Assert.Equal("Jane", e.After["first_name"]);
                Assert.Empty(e.Before);
            }
        }
        finally
        {
            await fixture.Container.ExecScriptAsync($"SELECT pg_drop_replication_slot('{slotName}');");
            await fixture.Container.ExecScriptAsync($"DROP TABLE {tableName};");
        }
    }

    [Fact]
    public async Task Update_ShouldProduceUpdateChangeEvent_WithBeforeAndAfter_WhenReplicaIdentityFull()
    {
        var tableName = $"test_table_{Guid.NewGuid():N}";
        var slotName = $"test_slot_{Guid.NewGuid():N}";

        try
        {
            await fixture.Container.ExecScriptAsync(
                $"CREATE TABLE {tableName} (id int PRIMARY KEY, first_name VARCHAR(100));");
            await fixture.Container.ExecScriptAsync(
                $"ALTER TABLE {tableName} REPLICA IDENTITY FULL;");
            await fixture.Container.ExecScriptAsync(
                $"INSERT INTO {tableName} VALUES (1, 'John');");
            await fixture.Container.ExecScriptAsync(
                $"SELECT pg_create_logical_replication_slot('{slotName}', 'pgoutput');");

            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var replicationOptions = new PostgresReplicationOptions(
                fixture.Container.GetConnectionString(), slotName, PostgresReplicationFixture.PublicationName);
            
            var replicationSource = new PostgresReplicationSource();
            var streamingTask = StreamAndAssertAsync(replicationSource, replicationOptions, AssertAction, cts.Token);

            var execResult = await fixture.Container.ExecScriptAsync(
                $"UPDATE {tableName} SET first_name = 'Jane' WHERE id = 1;");
            if (execResult.ExitCode != 0)
                Assert.Fail($"Failed to update row: {execResult.Stderr}");

            try { await streamingTask; }
            catch (OperationCanceledException) when (cts.IsCancellationRequested)
            { Assert.Fail("No change event received within timeout"); }

            void AssertAction(RawChangeEvent e)
            {
                Assert.Equal(ChangeOperation.Update, e.Operation);
                Assert.Equal(tableName, e.TableName);
                Assert.Equal("John", e.Before["first_name"]);
                Assert.Equal("Jane", e.After["first_name"]);
                Assert.NotEmpty(e.Before);
            }
        }
        finally
        {
            await fixture.Container.ExecScriptAsync($"SELECT pg_drop_replication_slot('{slotName}');");
            await fixture.Container.ExecScriptAsync($"DROP TABLE {tableName};");
        }
    }

    // ─── Delete ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task Delete_ShouldProduceDeleteChangeEvent_WithOnlyKeyInBefore_WhenReplicaIdentityDefault()
    {
        var tableName = $"test_table_{Guid.NewGuid():N}";
        var slotName = $"test_slot_{Guid.NewGuid():N}";

        try
        {
            await fixture.Container.ExecScriptAsync(
                $"CREATE TABLE {tableName} (id int PRIMARY KEY, first_name VARCHAR(100));");
            await fixture.Container.ExecScriptAsync(
                $"INSERT INTO {tableName} VALUES (1, 'John');");
            await fixture.Container.ExecScriptAsync(
                $"SELECT pg_create_logical_replication_slot('{slotName}', 'pgoutput');");

            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var replicationOptions = new PostgresReplicationOptions(
                fixture.Container.GetConnectionString(), slotName, PostgresReplicationFixture.PublicationName);

            var replicationSource = new PostgresReplicationSource();
            
            var streamingTask = StreamAndAssertAsync(replicationSource, replicationOptions, AssertAction, cts.Token);

            var execResult = await fixture.Container.ExecScriptAsync(
                $"DELETE FROM {tableName} WHERE id = 1;");
            if (execResult.ExitCode != 0)
                Assert.Fail($"Failed to delete row: {execResult.Stderr}");

            try { await streamingTask; }
            catch (OperationCanceledException) when (cts.IsCancellationRequested)
            { Assert.Fail("No change event received within timeout"); }

            void AssertAction(RawChangeEvent e)
            {
                Assert.Equal(ChangeOperation.Delete, e.Operation);
                Assert.Equal(tableName, e.TableName);
                Assert.Equal(1, Convert.ToInt32(e.Before["id"]));
                Assert.Empty(e.After);
            }
        }
        finally
        {
            await fixture.Container.ExecScriptAsync($"SELECT pg_drop_replication_slot('{slotName}');");
            await fixture.Container.ExecScriptAsync($"DROP TABLE {tableName};");
        }
    }

    [Fact]
    public async Task Delete_ShouldProduceDeleteChangeEvent_WithFullOldRow_WhenReplicaIdentityFull()
    {
        var tableName = $"test_table_{Guid.NewGuid():N}";
        var slotName = $"test_slot_{Guid.NewGuid():N}";

        try
        {
            await fixture.Container.ExecScriptAsync(
                $"CREATE TABLE {tableName} (id int PRIMARY KEY, first_name VARCHAR(100));");
            await fixture.Container.ExecScriptAsync(
                $"ALTER TABLE {tableName} REPLICA IDENTITY FULL;");
            await fixture.Container.ExecScriptAsync(
                $"INSERT INTO {tableName} VALUES (1, 'John');");
            await fixture.Container.ExecScriptAsync(
                $"SELECT pg_create_logical_replication_slot('{slotName}', 'pgoutput');");

            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var replicationOptions = new PostgresReplicationOptions(
                fixture.Container.GetConnectionString(), slotName, PostgresReplicationFixture.PublicationName);

            var replicationSource = new PostgresReplicationSource();
            
            var streamingTask = StreamAndAssertAsync(replicationSource, replicationOptions, AssertAction, cts.Token);

            var execResult = await fixture.Container.ExecScriptAsync(
                $"DELETE FROM {tableName} WHERE id = 1;");
            if (execResult.ExitCode != 0)
                Assert.Fail($"Failed to delete row: {execResult.Stderr}");

            try { await streamingTask; }
            catch (OperationCanceledException) when (cts.IsCancellationRequested)
            { Assert.Fail("No change event received within timeout"); }

            void AssertAction(RawChangeEvent e)
            {
                Assert.Equal(ChangeOperation.Delete, e.Operation);
                Assert.Equal(tableName, e.TableName);
                Assert.Equal(1, Convert.ToInt32(e.Before["id"]));
                Assert.Equal("John", e.Before["first_name"]);
                Assert.NotEmpty(e.Before);
                Assert.Empty(e.After);
            }
        }
        finally
        {
            await fixture.Container.ExecScriptAsync($"SELECT pg_drop_replication_slot('{slotName}');");
            await fixture.Container.ExecScriptAsync($"DROP TABLE {tableName};");
        }
    }

    // ─── LSN Confirmation ─────────────────────────────────────────────────────

    [Fact]
    public async Task ConfirmAsync_ShouldNotThrow_WhenCalledWithValidOffset()
    {
        var tableName = $"test_table_{Guid.NewGuid():N}";
        var slotName = $"test_slot_{Guid.NewGuid():N}";

        try
        {
            await fixture.Container.ExecScriptAsync(
                $"CREATE TABLE {tableName} (id int PRIMARY KEY);");
            await fixture.Container.ExecScriptAsync(
                $"SELECT pg_create_logical_replication_slot('{slotName}', 'pgoutput');");

            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var replicationOptions = new PostgresReplicationOptions(
                fixture.Container.GetConnectionString(), slotName, PostgresReplicationFixture.PublicationName);

            var replicationSource = new PostgresReplicationSource();
            
            var streamingTask = StreamAndAssertAsync(replicationSource, replicationOptions, e =>
            {
                replicationSource.ConfirmAsync(e.Offset, CancellationToken.None).Wait(cts.Token);
            }, cts.Token);

            await fixture.Container.ExecScriptAsync($"INSERT INTO {tableName} VALUES (1);");

            try { await streamingTask; }
            catch (OperationCanceledException) when (cts.IsCancellationRequested)
            { Assert.Fail("No change event received within timeout"); }
        }
        finally
        {
            await fixture.Container.ExecScriptAsync($"SELECT pg_drop_replication_slot('{slotName}');");
            await fixture.Container.ExecScriptAsync($"DROP TABLE {tableName};");
        }
    }

    [Fact]
    public async Task ConfirmAsync_ShouldThrowInvalidOperationException_WhenCalledBeforeStreamAsync()
    {
        var replicationSource = new PostgresReplicationSource();
        var offset = new PostgresReplicationOffset(default);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            replicationSource.ConfirmAsync(offset, CancellationToken.None));
    }

    // ─── Slot and Connection Behavior ─────────────────────────────────────────

    [Fact]
    public async Task StreamAsync_ShouldThrowInvalidOperationException_WhenCalledTwice()
    {
        var tableName = $"test_table_{Guid.NewGuid():N}";
        var slotName = $"test_slot_{Guid.NewGuid():N}";

        try
        {
            await fixture.Container.ExecScriptAsync(
                $"CREATE TABLE {tableName} (id int PRIMARY KEY);");
            await fixture.Container.ExecScriptAsync(
                $"SELECT pg_create_logical_replication_slot('{slotName}', 'pgoutput');");

            var replicationOptions = new PostgresReplicationOptions(
                fixture.Container.GetConnectionString(), slotName, PostgresReplicationFixture.PublicationName);

            var replicationSource = new PostgresReplicationSource();
            
            var cts = new CancellationTokenSource();
            
            var enumerator = replicationSource.StreamAsync(replicationOptions, cts.Token).GetAsyncEnumerator(cts.Token);
            var moveNextTask = enumerator.MoveNextAsync(); // This enters StreamAsync and blocks at StartReplication

            // At this point, _connection is set, lock logic has run
            // Second call WILL throw

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await foreach (var _ in replicationSource.StreamAsync(replicationOptions, CancellationToken.None))
                    break;
            });

            await cts.CancelAsync();
            try { await moveNextTask; } catch (OperationCanceledException) { }

        }
        finally
        {
            await fixture.Container.ExecScriptAsync($"SELECT pg_drop_replication_slot('{slotName}');");
            await fixture.Container.ExecScriptAsync($"DROP TABLE {tableName};");
        }
    }

    [Fact]
    public async Task StreamAsync_ShouldStopCleanly_WhenCancelled()
    {
        var tableName = $"test_table_{Guid.NewGuid():N}";
        var slotName = $"test_slot_{Guid.NewGuid():N}";

        try
        {
            await fixture.Container.ExecScriptAsync(
                $"CREATE TABLE {tableName} (id int PRIMARY KEY);");
            await fixture.Container.ExecScriptAsync(
                $"SELECT pg_create_logical_replication_slot('{slotName}', 'pgoutput');");

            var replicationOptions = new PostgresReplicationOptions(
                fixture.Container.GetConnectionString(), slotName, PostgresReplicationFixture.PublicationName);

            var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
            var replicationSource = new PostgresReplicationSource();

            var exception = await Record.ExceptionAsync(async () =>
            {
                await foreach (var _ in replicationSource.StreamAsync(replicationOptions, cts.Token)) { }
            });

            Assert.True(exception is null or OperationCanceledException);
        }
        finally
        {
            await fixture.Container.ExecScriptAsync($"SELECT pg_drop_replication_slot('{slotName}');");
            await fixture.Container.ExecScriptAsync($"DROP TABLE {tableName};");
        }
    }

    // ─── Multiple Tables ──────────────────────────────────────────────────────

    [Fact]
    public async Task StreamAsync_ShouldSetCorrectTableName_ForMultipleTables()
    {
        var tableName1 = $"test_table_{Guid.NewGuid():N}";
        var tableName2 = $"test_table_{Guid.NewGuid():N}";
        var slotName = $"test_slot_{Guid.NewGuid():N}";

        try
        {
            await fixture.Container.ExecScriptAsync(
                $"CREATE TABLE {tableName1} (id int PRIMARY KEY);");
            await fixture.Container.ExecScriptAsync(
                $"CREATE TABLE {tableName2} (id int PRIMARY KEY);");
            await fixture.Container.ExecScriptAsync(
                $"SELECT pg_create_logical_replication_slot('{slotName}', 'pgoutput');");

            var replicationOptions = new PostgresReplicationOptions(
                fixture.Container.GetConnectionString(), slotName, PostgresReplicationFixture.PublicationName);

            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var replicationSource = new PostgresReplicationSource();
            var receivedTableNames = new List<string>();

            var streamingTask = Task.Run(async () =>
            {
                await foreach (var e in replicationSource.StreamAsync(replicationOptions, cts.Token))
                {
                    receivedTableNames.Add(e.TableName);
                    await replicationSource.ConfirmAsync(e.Offset, cts.Token);
                    if (receivedTableNames.Count == 2) break;
                }
            });

            await fixture.Container.ExecScriptAsync($"INSERT INTO {tableName1} VALUES (1);");
            await fixture.Container.ExecScriptAsync($"INSERT INTO {tableName2} VALUES (1);");

            try { await streamingTask; }
            catch (OperationCanceledException) when (cts.IsCancellationRequested)
            { Assert.Fail("No change event received within timeout"); }

            Assert.Contains(tableName1, receivedTableNames);
            Assert.Contains(tableName2, receivedTableNames);
        }
        finally
        {
            await fixture.Container.ExecScriptAsync($"SELECT pg_drop_replication_slot('{slotName}');");
            await fixture.Container.ExecScriptAsync($"DROP TABLE {tableName1};");
            await fixture.Container.ExecScriptAsync($"DROP TABLE {tableName2};");
        }
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private static async Task StreamAndAssertAsync(
        PostgresReplicationSource replicationSource,
        PostgresReplicationOptions replicationOptions,
        Action<RawChangeEvent> assert,
        CancellationToken cancellationToken)
    {
        await foreach (var rawChangeEvent in replicationSource.StreamAsync(replicationOptions, cancellationToken))
        {
            try
            {
                assert(rawChangeEvent);
                return;
            }
            finally
            {
                await replicationSource.ConfirmAsync(rawChangeEvent.Offset, cancellationToken);
            }
        }

        Assert.Fail("Stream completed without yielding a change event.");
    }
}