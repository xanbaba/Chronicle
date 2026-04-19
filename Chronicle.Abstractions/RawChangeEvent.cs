namespace Chronicle.Abstractions;

/// <summary>
/// A database-agnostic representation of a row-level change event.
/// This is the intermediate format that flows from providers into Core,
/// before being mapped to a strongly-typed <c>ChangeEvent&lt;T&gt;</c>.
/// </summary>
/// <param name="SchemaName">The database schema containing the table (e.g., "public", "dbo").</param>
/// <param name="TableName">The name of the table where the change occurred.</param>
/// <param name="Operation">The type of change that occurred.</param>
/// <param name="Before">
/// The state of the row before the change. Empty for inserts. 
/// Availability of old row data for updates and deletes depends on the database and provider configuration.
/// </param>
/// <param name="After">
/// The state of the row after the change. Empty for deletes.
/// </param>
/// <param name="Offset">
/// The position in the database's transaction log where this change occurred.
/// Used for acknowledging processed changes and resuming from a specific position.
/// </param>
/// <remarks>
/// <para>
/// Column values in <see cref="Before"/> and <see cref="After"/> are stringly-typed dictionaries
/// where keys are column names and values are the column values as objects. Type safety is added
/// later by the mapper in Chronicle.Core.
/// </para>
/// <para>
/// The completeness of <see cref="Before"/> and <see cref="After"/> data varies by database provider
/// and its configuration. Consult provider-specific documentation for details.
/// </para>
/// </remarks>
public record RawChangeEvent(
    string SchemaName,
    string TableName,
    ChangeOperation Operation,
    IReadOnlyDictionary<string, object?> Before,
    IReadOnlyDictionary<string, object?> After,
    ReplicationOffset Offset
);