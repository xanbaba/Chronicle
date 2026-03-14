using Chronicle.Abstractions;

namespace Chronicle.Postgres;

public record PostgresReplicationOptions(string ConnectionString, string SlotName, string PublicationName)
    : ReplicationOptions(ConnectionString);