using System.Collections.Immutable;

namespace MimironSQL.DbContextGenerator;

internal sealed class KeySpec(ImmutableArray<string> columnNames)
{
    public ImmutableArray<string> ColumnNames { get; } = columnNames;
}
