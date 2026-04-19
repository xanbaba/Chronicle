namespace Chronicle.Abstractions;

/// <summary>
/// Base configuration for connecting to a replication source.
/// Providers extend this record with database-specific options.
/// </summary>
/// <param name="ConnectionString">The connection string for the database.</param>
/// <remarks>
/// <para>
/// This base class contains only the universal field — connection string.
/// Provider-specific options (e.g., slot name, publication name for PostgreSQL)
/// are added via inheritance.
/// </para>
/// <para>
/// Provider implementations:
/// <list type="bullet">
///   <item><description>PostgreSQL: <c>PostgresReplicationOptions</c> adds <c>SlotName</c> and <c>PublicationName</c>.</description></item>
/// </list>
/// </para>
/// </remarks>
public record ReplicationOptions(string ConnectionString);