using Chronicle.Abstractions;
using NpgsqlTypes;

namespace Chronicle.Postgres;

/// <summary>
/// Represents a position in the PostgreSQL Write-Ahead Log (WAL) using a Log Sequence Number (LSN).
/// </summary>
/// <param name="WalEnd">The LSN position in the WAL. This is the position to confirm back to PostgreSQL.</param>
/// <remarks>
/// <para>
/// PostgreSQL uses LSN (Log Sequence Number) as a 64-bit position marker in the WAL.
/// Confirming an LSN tells PostgreSQL that all changes up to that point have been processed,
/// allowing it to release WAL segments for deletion.
/// </para>
/// <para>
/// This offset is obtained from <c>RawChangeEvent.Offset</c> and passed to
/// <c>PostgresReplicationSource.ConfirmAsync</c> to acknowledge processed changes.
/// </para>
/// </remarks>
internal record PostgresReplicationOffset(NpgsqlLogSequenceNumber WalEnd) : ReplicationOffset;