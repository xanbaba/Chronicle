using System.Runtime.CompilerServices;
using Chronicle.Abstractions;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;

namespace Chronicle.Postgres;

/// <summary>
/// PostgreSQL implementation of <see cref="IReplicationSource"/> using logical replication
/// with the pgoutput protocol.
/// </summary>
/// <remarks>
/// <para>
/// This class connects to PostgreSQL's logical replication stream and decodes change events
/// from the Write-Ahead Log (WAL). It uses Npgsql's <c>LogicalReplicationConnection</c>
/// and the pgoutput plugin built into PostgreSQL 10+.
/// </para>
/// <para>
/// <strong>Thread safety:</strong> A single <see cref="PostgresReplicationSource"/> instance
/// can only have one active stream at a time. Calling <see cref="StreamAsync"/> while a stream
/// is already running throws <see cref="InvalidOperationException"/>.
/// </para>
/// <para>
/// <strong>LSN confirmation:</strong> Call <see cref="ConfirmAsync"/> after processing each event
/// to allow PostgreSQL to release WAL segments. Failure to confirm causes WAL to accumulate
/// and may fill disk space.
/// </para>
/// <para>
/// <strong>Replica Identity:</strong> The amount of data in <c>Before</c> and <c>After</c>
/// depends on the table's replica identity setting:
/// <list type="table">
///   <listheader>
///     <term>Setting</term>
///     <description>Before/After availability</description>
///   </listheader>
///   <item>
///     <term>DEFAULT</term>
///     <description>Before is empty for updates, only key columns for deletes.</description>
///   </item>
///   <item>
///     <term>FULL</term>
///     <description>Before contains the complete old row for updates and deletes.</description>
///   </item>
///   <item>
///     <term>USING INDEX</term>
///     <description>Before contains the index columns for updates and deletes.</description>
///   </item>
/// </list>
/// </para>
/// </remarks>
public class PostgresReplicationSource : IReplicationSource
{
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly PgOutputDecoder _pgOutputDecoder = new();
    private LogicalReplicationConnection? _connection;

    /// <summary>
    /// Opens a replication connection and streams change events from PostgreSQL.
    /// </summary>
    /// <param name="options">
    /// Must be a <see cref="PostgresReplicationOptions"/> instance with connection details,
    /// slot name, and publication name.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the stream.</param>
    /// <returns>
    /// An async enumerable of <see cref="RawChangeEvent"/> objects representing
    /// row-level changes. The stream is infinite until cancelled.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="options"/> is not a <see cref="PostgresReplicationOptions"/>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <see cref="StreamAsync"/> is called while a stream is already active
    /// on this instance.
    /// </exception>
    public async IAsyncEnumerable<RawChangeEvent> StreamAsync(ReplicationOptions options,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (options is not PostgresReplicationOptions postgresOptions)
        {
            throw new ArgumentException($"{nameof(options)} must be of type {nameof(PostgresReplicationOptions)}");
        }

        await _connectionLock.WaitAsync(cancellationToken);

        try
        {
            if (_connection is not null)
            {
                throw new InvalidOperationException($"{nameof(StreamAsync)} is already running.");
            }
        
            _connection = new LogicalReplicationConnection(postgresOptions.ConnectionString);
            await _connection.Open(cancellationToken);
        }
        finally
        {
            _connectionLock.Release();
        }

        try
        {
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

    /// <summary>
    /// Confirms that changes up to the specified LSN have been processed,
    /// allowing PostgreSQL to release WAL segments.
    /// </summary>
    /// <param name="offset">
    /// Must be a <see cref="PostgresReplicationOffset"/> containing the LSN to confirm.
    /// Typically obtained from <c>RawChangeEvent.Offset</c>.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A completed task.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when called before <see cref="StreamAsync"/> has established a connection.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="offset"/> is not a <see cref="PostgresReplicationOffset"/>.
    /// </exception>
    public Task ConfirmAsync(ReplicationOffset offset, CancellationToken cancellationToken)
    {
        if (_connection is null)
        {
            throw new InvalidOperationException(
                $"{nameof(_connection)} is not initialized. Call {nameof(StreamAsync)}() first.");
        }

        if (offset is not PostgresReplicationOffset replicationOffset)
        {
            throw new ArgumentException(
                $"{nameof(offset)} must be of type {nameof(PostgresReplicationOffset)}");
        }

        _connection.SetReplicationStatus(replicationOffset.WalEnd);
        return Task.CompletedTask;
    }
}