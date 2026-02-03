namespace MimironSQL.DbContextGenerator;

internal sealed class ColumnSpec(string dbdType, string name, int? arrayLength)
{
    public string DbdType { get; } = dbdType;
    public string Name { get; } = name;
    public int? ArrayLength { get; } = arrayLength;
}
