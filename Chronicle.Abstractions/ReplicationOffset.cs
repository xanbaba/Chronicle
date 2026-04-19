namespace Chronicle.Abstractions;

/// <summary>
/// An opaque representation of a position in a database's transaction log.
/// Each provider implements a subtype that wraps the database-specific position type.
/// </summary>
/// <remarks>
/// <para>
/// Core passes <see cref="ReplicationOffset"/> objects between components without inspecting them.
/// Only the provider that created an offset knows how to interpret it.
/// </para>
/// <para>
/// Provider implementations:
/// <list type="bullet">
///   <item><description>PostgreSQL: <c>PostgresReplicationOffset</c> wraps <c>NpgsqlLogSequenceNumber</c> (LSN).</description></item>
///   <item><description>SQL Server: Would wrap a CDC position or LSN equivalent.</description></item>
/// </list>
/// </para>
/// </remarks>
public abstract record ReplicationOffset;