using System.Collections.Immutable;

namespace CASC.Net.Generators;

internal sealed record ParsedTable(
	string TableName,
	ImmutableArray<ColumnSpec> Columns,
	ImmutableArray<KeySpec> Keys,
	ImmutableArray<ForeignKeySpec> ForeignKeys,
	ImmutableArray<DbdVersionDefinition> Versions);

internal sealed record DbdVersionDefinition(
	ImmutableArray<uint> LayoutHashes,
	ImmutableArray<DbdBuildSpec> Builds,
	ImmutableArray<DbdPhysicalColumnSpec> Columns);

internal abstract record DbdBuildSpec
{
	public sealed record Exact(string Version) : DbdBuildSpec;

	public sealed record Range(string From, string To) : DbdBuildSpec;
}

internal sealed record DbdPhysicalColumnSpec(
	string ColumnName,
	int? ArrayLength,
	bool IsId,
	bool IsRelation,
	bool IsNonInline);
