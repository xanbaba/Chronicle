namespace Chronicle.Abstractions;

/// <summary>
/// The contract every database provider must implement. Produces a stream of
/// row-level change events from a database's transaction log.
/// </summary>
/// <remarks>
/// <para>
/// This interface is the boundary between Chronicle.Core and database-specific implementations.
/// Core receives <see cref="RawChangeEvent"/> objects from providers and handles all downstream
/// processing — mapping, filtering, fan-out, and delivery to consumers.
/// </para>
/// <para>
/// Implementations should handle:
/// <list type="bullet">
///   <item><description>Opening and maintaining the replication connection.</description></item>
///   <item><description>Decoding database-specific binary protocols into <see cref="RawChangeEvent"/>.</description></item>
///   <item><description>Managing replication slots or equivalent cursor mechanisms.</description></item>
///   <item><description>Tracking and confirming positions in the log.</description></item>
/// </list>
/// </para>
/// </remarks>
public interface IReplicationSource
{
    /// <summary>
    /// Opens a replication connection and streams change events from the database.
    /// </summary>
    /// <param name="options">
    /// Provider-specific configuration. Must be cast to the appropriate subtype
    /// (e.g., <c>PostgresReplicationOptions</c>) by the implementation.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the stream.</param>
    /// <returns>
    /// An async enumerable of <see cref="RawChangeEvent"/> objects representing
    /// row-level changes in the database. The stream is infinite until cancelled.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="options"/> is not of the expected provider-specific type.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <see cref="StreamAsync"/> is called while a stream is already active.
    /// </exception>
    public IAsyncEnumerable<RawChangeEvent> StreamAsync(ReplicationOptions options, CancellationToken cancellationToken);
    
    /// <summary>
    /// Confirms that changes up to the specified offset have been processed, allowing the database
    /// to release resources (e.g., WAL segments in PostgreSQL).
    /// </summary>
    /// <param name="offset">
    /// The position in the log to confirm. Must be a provider-specific subtype
    /// (e.g., <c>PostgresReplicationOffset</c>).
    /// </param>
    /// <param name="cancellationToken">A token to cancel the confirmation.</param>
    /// <returns>A task representing the asynchronous confirmation operation.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when called before <see cref="StreamAsync"/> has established a connection.
    /// </exception>
    public Task ConfirmAsync(ReplicationOffset offset, CancellationToken cancellationToken);
}