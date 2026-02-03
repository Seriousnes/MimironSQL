using System.Collections.Immutable;

namespace MimironSQL.DbContextGenerator;

internal sealed class ParsedTable(
    string tableName,
    ImmutableArray<ColumnSpec> columns,
    ImmutableArray<KeySpec> keys,
    ImmutableArray<ForeignKeySpec> foreignKeys,
    ImmutableArray<DbdVersionDefinition> versions)
{
    public string TableName { get; } = tableName;
    public ImmutableArray<ColumnSpec> Columns { get; } = columns;
    public ImmutableArray<KeySpec> Keys { get; } = keys;
    public ImmutableArray<ForeignKeySpec> ForeignKeys { get; } = foreignKeys;
    public ImmutableArray<DbdVersionDefinition> Versions { get; } = versions;
}

internal sealed class DbdVersionDefinition(
    ImmutableArray<uint> layoutHashes,
    ImmutableArray<DbdBuildSpec> builds,
    ImmutableArray<DbdPhysicalColumnSpec> columns)
{
    public ImmutableArray<uint> LayoutHashes { get; } = layoutHashes;
    public ImmutableArray<DbdBuildSpec> Builds { get; } = builds;
    public ImmutableArray<DbdPhysicalColumnSpec> Columns { get; } = columns;
}

internal abstract class DbdBuildSpec
{
    public sealed class Exact(string version) : DbdBuildSpec
    {
        public string Version { get; } = version;
    }

    public sealed class Range(string from, string to) : DbdBuildSpec
    {
        public string From { get; } = from;
        public string To { get; } = to;
    }
}

internal sealed class DbdPhysicalColumnSpec(
    string columnName,
    int? arrayLength,
    bool isId,
    bool isRelation,
    bool isNonInline)
{
    public string ColumnName { get; } = columnName;
    public int? ArrayLength { get; } = arrayLength;
    public bool IsId { get; } = isId;
    public bool IsRelation { get; } = isRelation;
    public bool IsNonInline { get; } = isNonInline;
}
