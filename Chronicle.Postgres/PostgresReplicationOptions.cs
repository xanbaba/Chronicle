using Chronicle.Abstractions;

namespace Chronicle.Postgres;

internal record PostgresReplicationOptions(string ConnectionString, string SlotName, string PublicationName)
    : ReplicationOptions(ConnectionString);