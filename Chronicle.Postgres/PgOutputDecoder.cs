using Chronicle.Abstractions;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

namespace Chronicle.Postgres;

internal class PgOutputDecoder
{
    private async Task<IReadOnlyDictionary<string, object?>> ReadRowsAsync(ReplicationTuple rows, CancellationToken cancellationToken = default)
    {
        var rowsDictionary = new Dictionary<string, object?>();
        await foreach (var replicationValue in rows)
        {
            rowsDictionary[replicationValue.GetFieldName()] = await replicationValue.Get(cancellationToken);
        }

        return rowsDictionary;
    }

    public async Task<RawChangeEvent?> DecodeAsync(PgOutputReplicationMessage message, CancellationToken cancellationToken = default)
    {
        if (message is InsertMessage insertMessage)
        {
            var insertMessageRelation = insertMessage.Relation;

            var rawChangeEvent = new RawChangeEvent(
                SchemaName: insertMessageRelation.Namespace,
                TableName: insertMessageRelation.RelationName,
                Operation: ChangeOperation.Insert,
                Before: new Dictionary<string, object?>(),
                After: await ReadRowsAsync(insertMessage.NewRow, cancellationToken),
                Offset: new PostgresReplicationOffset(insertMessage.WalEnd)
            );
            return rawChangeEvent;
        }

        if (message is FullUpdateMessage fullUpdateMessage)
        {
            var updateMessageRelation = fullUpdateMessage.Relation;

            var rawChangeEvent = new RawChangeEvent(
                SchemaName: updateMessageRelation.Namespace,
                TableName: updateMessageRelation.RelationName,
                Operation: ChangeOperation.Update,
                Before: await ReadRowsAsync(fullUpdateMessage.OldRow, cancellationToken),
                After: await ReadRowsAsync(fullUpdateMessage.NewRow, cancellationToken),
                Offset: new PostgresReplicationOffset(fullUpdateMessage.WalEnd)
            );
            return rawChangeEvent;
        }

        if (message is DefaultUpdateMessage defaultUpdateMessage)
        {
            var defaultUpdateMessageRelation = defaultUpdateMessage.Relation;

            var rawChangeEvent = new RawChangeEvent(
                SchemaName: defaultUpdateMessageRelation.Namespace,
                TableName: defaultUpdateMessageRelation.RelationName,
                Operation: ChangeOperation.Update,
                Before: new Dictionary<string, object?>(),
                After: await ReadRowsAsync(defaultUpdateMessage.NewRow, cancellationToken),
                Offset: new PostgresReplicationOffset(defaultUpdateMessage.WalEnd)
            );
            return rawChangeEvent;
        }

        if (message is FullDeleteMessage fullDeleteMessage)
        {
            var fullDeleteMessageRelation = fullDeleteMessage.Relation;

            var rawChangeEvent = new RawChangeEvent(
                SchemaName: fullDeleteMessageRelation.Namespace,
                TableName: fullDeleteMessageRelation.RelationName,
                Operation: ChangeOperation.Delete,
                Before: await ReadRowsAsync(fullDeleteMessage.OldRow, cancellationToken),
                After: new Dictionary<string, object?>(),
                Offset: new PostgresReplicationOffset(fullDeleteMessage.WalEnd)
            );
            return rawChangeEvent;
        }

        if (message is KeyDeleteMessage keyDeleteMessage)
        {
            var  keyDeleteMessageRelation = keyDeleteMessage.Relation;

            var rawChangeEvent = new RawChangeEvent(
                SchemaName: keyDeleteMessageRelation.Namespace,
                TableName: keyDeleteMessageRelation.RelationName,
                Operation: ChangeOperation.Delete,
                Before: await ReadRowsAsync(keyDeleteMessage.Key, cancellationToken),
                After: new Dictionary<string, object?>(),
                Offset: new PostgresReplicationOffset(keyDeleteMessage.WalEnd)
            );
            return rawChangeEvent;
        }

        return null;
    }
}