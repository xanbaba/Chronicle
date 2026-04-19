using Chronicle.Abstractions;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

namespace Chronicle.Postgres;

/// <summary>
/// Decodes PostgreSQL pgoutput protocol messages into <see cref="RawChangeEvent"/> objects.
/// Internal to the PostgreSQL provider — not exposed to consumers.
/// </summary>
/// <remarks>
/// <para>
/// This decoder handles the pgoutput message stream from Npgsql and translates each
/// message type into the appropriate <see cref="RawChangeEvent"/> representation.
/// </para>
/// <para>
/// Message types handled:
/// <list type="bullet">
///   <item><description>InsertMessage — Before is empty, After contains the new row.</description></item>
///   <item><description>FullUpdateMessage — Both Before and After are populated (REPLICA IDENTITY FULL).</description></item>
///   <item><description>IndexUpdateMessage — Before contains key columns, After contains the new row.</description></item>
///   <item><description>DefaultUpdateMessage — Before is empty, After contains the new row (REPLICA IDENTITY DEFAULT).</description></item>
///   <item><description>FullDeleteMessage — Before contains the deleted row, After is empty.</description></item>
///   <item><description>KeyDeleteMessage — Before contains key columns only, After is empty.</description></item>
/// </list>
/// </para>
/// <para>
/// Messages not resulting in a change event (Begin, Commit, Relation) return <c>null</c>.
/// </para>
/// </remarks>
internal class PgOutputDecoder
{
    private static readonly IReadOnlyDictionary<string, object?> EmptyRow =
        new Dictionary<string, object?>();

    /// <summary>
    /// Reads column values from a replication tuple into a dictionary.
    /// </summary>
    /// <param name="rows">The replication tuple containing column data.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A dictionary mapping column names to their values.</returns>
    /// <remarks>
    /// <para>
    /// <strong>TOAST handling:</strong> PostgreSQL uses TOAST (The Oversized Attribute Storage Technique)
    /// for large column values. When a TOAST value is unchanged in an update, PostgreSQL does not send
    /// the actual data — instead, it provides a pointer. In this case, <c>IsUnchangedToastedValue</c> is true
    /// and the value is set to <c>null</c>.
    /// </para>
    /// <para>
    /// To receive complete before/after values for large columns, set <c>REPLICA IDENTITY FULL</c> on the table:
    /// <code>ALTER TABLE my_table REPLICA IDENTITY FULL;</code>
    /// Note that this increases WAL volume significantly for tables with large columns.
    /// </para>
    /// </remarks>
    private async Task<IReadOnlyDictionary<string, object?>> ReadRowsAsync(ReplicationTuple rows,
        CancellationToken cancellationToken = default)
    {
        var rowsDictionary = new Dictionary<string, object?>();
        await foreach (var replicationValue in rows.WithCancellation(cancellationToken))
        {
            if (replicationValue.IsUnchangedToastedValue)
                rowsDictionary[replicationValue.GetFieldName()] = null;
            else
                rowsDictionary[replicationValue.GetFieldName()] = await replicationValue.Get(cancellationToken);
        }

        return rowsDictionary;
    }

    /// <summary>
    /// Decodes a pgoutput replication message into a <see cref="RawChangeEvent"/>.
    /// </summary>
    /// <param name="message">The replication message from Npgsql.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>
    /// A <see cref="RawChangeEvent"/> for data messages (Insert, Update, Delete),
    /// or <c>null</c> for transaction control messages (Begin, Commit, Relation).
    /// </returns>
    public async Task<RawChangeEvent?> DecodeAsync(PgOutputReplicationMessage message,
        CancellationToken cancellationToken = default)
    {
        if (message is InsertMessage insertMessage)
        {
            var insertMessageRelation = insertMessage.Relation;

            var rawChangeEvent = new RawChangeEvent(
                SchemaName: insertMessageRelation.Namespace,
                TableName: insertMessageRelation.RelationName,
                Operation: ChangeOperation.Insert,
                Before: EmptyRow,
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

        if (message is IndexUpdateMessage indexUpdateMessage)
        {
            var indexUpdateMessageRelation = indexUpdateMessage.Relation;

            var rawChangeEvent = new RawChangeEvent(
                SchemaName: indexUpdateMessageRelation.Namespace,
                TableName: indexUpdateMessageRelation.RelationName,
                Operation: ChangeOperation.Update,
                Before: await ReadRowsAsync(indexUpdateMessage.Key, cancellationToken),
                After: await ReadRowsAsync(indexUpdateMessage.NewRow, cancellationToken),
                Offset: new PostgresReplicationOffset(indexUpdateMessage.WalEnd)
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
                Before: EmptyRow,
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
                After: EmptyRow,
                Offset: new PostgresReplicationOffset(fullDeleteMessage.WalEnd)
            );
            return rawChangeEvent;
        }

        if (message is KeyDeleteMessage keyDeleteMessage)
        {
            var keyDeleteMessageRelation = keyDeleteMessage.Relation;

            var rawChangeEvent = new RawChangeEvent(
                SchemaName: keyDeleteMessageRelation.Namespace,
                TableName: keyDeleteMessageRelation.RelationName,
                Operation: ChangeOperation.Delete,
                Before: await ReadRowsAsync(keyDeleteMessage.Key, cancellationToken),
                After: EmptyRow,
                Offset: new PostgresReplicationOffset(keyDeleteMessage.WalEnd)
            );
            return rawChangeEvent;
        }

        return null;
    }
}