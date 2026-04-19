using Chronicle.Abstractions;

namespace Chronicle.Postgres;

/// <summary>
/// PostgreSQL-specific configuration for connecting to a logical replication stream.
/// </summary>
/// <param name="ConnectionString">The PostgreSQL connection string.</param>
/// <param name="SlotName">
/// The name of the replication slot to read from. Must already exist in PostgreSQL.
/// Create with: <code>SELECT pg_create_logical_replication_slot('slot_name', 'pgoutput');</code>
/// </param>
/// <param name="PublicationName">
/// The name of the publication defining which tables to replicate.
/// Create with: <code>CREATE PUBLICATION pub_name FOR ALL TABLES;</code>
/// </param>
/// <remarks>
/// <para>
/// <strong>Prerequisites:</strong>
/// <list type="bullet">
///   <item><description>PostgreSQL must have <c>wal_level = logical</c> configured.</description></item>
///   <item><description>The replication slot must exist before starting the stream.</description></item>
///   <item><description>The publication must include the tables you want to monitor.</description></item>
///   <item><description>The connecting user must have the REPLICATION attribute.</description></item>
/// </list>
/// </para>
/// </remarks>
internal record PostgresReplicationOptions(string ConnectionString, string SlotName, string PublicationName)
    : ReplicationOptions(ConnectionString);