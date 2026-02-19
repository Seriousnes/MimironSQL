namespace MimironSQL.EntityFrameworkCore.Schema;

internal sealed class Db2TableSchema(
    string tableName,
    int physicalColumnCount,
    IReadOnlyList<Db2FieldSchema> fields,
    IReadOnlyList<uint>? allowedLayoutHashes)
{
    public Db2TableSchema(
        string tableName,
        uint layoutHash,
        int physicalColumnCount,
        IReadOnlyList<Db2FieldSchema> fields)
        : this(
            tableName,
            physicalColumnCount,
            fields,
            allowedLayoutHashes: layoutHash == 0 ? null : [layoutHash])
    {
    }

    private readonly Dictionary<string, Db2FieldSchema> _fieldsByName = fields.ToDictionary(static f => f.Name, static f => f, StringComparer.Ordinal);

    public string TableName { get; } = tableName;
    public int PhysicalColumnCount { get; } = physicalColumnCount;
    public IReadOnlyList<Db2FieldSchema> Fields { get; } = fields;

    public IReadOnlyList<uint>? AllowedLayoutHashes { get; } = allowedLayoutHashes;

    public bool AllowsAnyLayoutHash => AllowedLayoutHashes is null;

    public bool IsLayoutHashAllowed(uint layoutHash)
        => AllowsAnyLayoutHash || AllowedLayoutHashes!.Contains(layoutHash);

    public bool TryGetField(string name, out Db2FieldSchema field)
    {
        return _fieldsByName.TryGetValue(name, out field);
    }

    public bool TryGetFieldCaseInsensitive(string name, out Db2FieldSchema field)
    {
        if (TryGetField(name, out field))
            return true;

        var caseInsensitiveMatch = Fields.FirstOrDefault(f => string.Equals(f.Name, name, StringComparison.OrdinalIgnoreCase));
        if (!caseInsensitiveMatch.Equals(default))
        {
            field = caseInsensitiveMatch;
            return true;
        }

        field = default;
        return false;
    }
}
