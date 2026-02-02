using System.Collections.Immutable;

namespace CASC.Net.Generators;

internal sealed record TableSpec(
	string TableName,
	int FileDataId,
	ImmutableArray<ColumnSpec> Columns,
	ImmutableArray<KeySpec> Keys,
	ImmutableArray<ForeignKeySpec> ForeignKeys,
	ImmutableArray<DbdVersionDefinition> Versions);
