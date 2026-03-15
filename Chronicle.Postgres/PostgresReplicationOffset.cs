using Chronicle.Abstractions;
using NpgsqlTypes;

namespace Chronicle.Postgres;

internal record PostgresReplicationOffset(NpgsqlLogSequenceNumber WalEnd) : ReplicationOffset;