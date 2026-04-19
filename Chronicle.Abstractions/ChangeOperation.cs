namespace Chronicle.Abstractions;

/// <summary>
/// Represents the type of change that occurred to a row in the database.
/// </summary>
public enum ChangeOperation
{
    /// <summary>
    /// A new row was inserted into the table.
    /// </summary>
    Insert,
    
    /// <summary>
    /// An existing row was updated in the table.
    /// </summary>
    Update,
    
    /// <summary>
    /// A row was deleted from the table.
    /// </summary>
    Delete
}