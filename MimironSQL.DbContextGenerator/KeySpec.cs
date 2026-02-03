using System.Collections.Immutable;

namespace MimironSQL.DbContextGenerator;

internal sealed class KeySpec
{
	public ImmutableArray<string> ColumnNames { get; }

	public KeySpec(ImmutableArray<string> columnNames)
	{
		ColumnNames = columnNames;
	}
}
