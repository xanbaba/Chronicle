using System.Runtime.CompilerServices;
using Chronicle.Abstractions;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;

namespace Chronicle.Postgres;

public class PostgresReplicationSource : IReplicationSource
{
    private readonly PgOutputDecoder _pgOutputDecoder = new();
    private LogicalReplicationConnection? _connection;

    public async IAsyncEnumerable<RawChangeEvent> StreamAsync(ReplicationOptions options,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (options is not PostgresReplicationOptions postgresOptions)
        {
            throw new ArgumentException($"{nameof(options)} must be of type {nameof(PostgresReplicationOptions)}");
        }

        if (_connection is not null)
        {
            throw new InvalidOperationException($"{nameof(StreamAsync)} is already running.");
        }

        try
        {
            _connection = new LogicalReplicationConnection(postgresOptions.ConnectionString);
            await _connection.Open(cancellationToken);

            var slot = new PgOutputReplicationSlot(postgresOptions.SlotName);
            var pgOutputOptions =
                new PgOutputReplicationOptions(postgresOptions.PublicationName, PgOutputProtocolVersion.V1);

            await foreach (var message in _connection.StartReplication(slot, pgOutputOptions, cancellationToken))
            {
                var rawChangeEvent = await _pgOutputDecoder.DecodeAsync(message, cancellationToken);
                if (rawChangeEvent is not null)
                {
                    yield return rawChangeEvent;
                }
                else
                {
                    _connection.SetReplicationStatus(message.WalEnd);
                }
            }
        }
        finally
        {
            if (_connection != null) await _connection.DisposeAsync();
            _connection = null;
        }
    }

    public Task ConfirmAsync(ReplicationOffset offset, CancellationToken cancellationToken)
    {
        if (_connection is null)
        {
            throw new InvalidOperationException(
                $"{nameof(_connection)} is not initialized. Call {nameof(StreamAsync)}() first.");
        }

        if (offset is not PostgresReplicationOffset replicationOffset)
        {
            throw new InvalidOperationException(
                $"{nameof(offset)} must be of type {nameof(PostgresReplicationOffset)}");
        }

        _connection.SetReplicationStatus(replicationOffset.WalEnd);
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection != null) await _connection.DisposeAsync();
    }
}