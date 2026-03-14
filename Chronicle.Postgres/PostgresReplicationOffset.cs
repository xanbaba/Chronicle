using Chronicle.Abstractions;
using NpgsqlTypes;

namespace Chronicle.Postgres;

public record PostgresReplicationOffset(NpgsqlLogSequenceNumber WalEnd) : ReplicationOffset;