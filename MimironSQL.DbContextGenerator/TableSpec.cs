using System.Collections.Immutable;

namespace MimironSQL.DbContextGenerator;

internal sealed class TableSpec(
    string tableName,
    int fileDataId,
    ImmutableArray<ColumnSpec> columns,
    ImmutableArray<KeySpec> keys,
    ImmutableArray<ForeignKeySpec> foreignKeys,
    ImmutableArray<DbdVersionDefinition> versions)
{
    public string TableName { get; } = tableName;
    public int FileDataId { get; } = fileDataId;
    public ImmutableArray<ColumnSpec> Columns { get; } = columns;
    public ImmutableArray<KeySpec> Keys { get; } = keys;
    public ImmutableArray<ForeignKeySpec> ForeignKeys { get; } = foreignKeys;
    public ImmutableArray<DbdVersionDefinition> Versions { get; } = versions;
}
