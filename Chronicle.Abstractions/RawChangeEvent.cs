namespace Chronicle.Abstractions;

public record RawChangeEvent(
    string SchemaName,
    string TableName,
    ChangeOperation Operation,
    IReadOnlyDictionary<string, object?> Before,
    IReadOnlyDictionary<string, object?> After
);