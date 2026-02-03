using System.Collections.Immutable;

namespace MimironSQL.DbContextGenerator;

internal sealed class TableSpec
{
	public string TableName { get; }
	public int FileDataId { get; }
	public ImmutableArray<ColumnSpec> Columns { get; }
	public ImmutableArray<KeySpec> Keys { get; }
	public ImmutableArray<ForeignKeySpec> ForeignKeys { get; }
	public ImmutableArray<DbdVersionDefinition> Versions { get; }

	public TableSpec(
		string tableName,
		int fileDataId,
		ImmutableArray<ColumnSpec> columns,
		ImmutableArray<KeySpec> keys,
		ImmutableArray<ForeignKeySpec> foreignKeys,
		ImmutableArray<DbdVersionDefinition> versions)
	{
		TableName = tableName;
		FileDataId = fileDataId;
		Columns = columns;
		Keys = keys;
		ForeignKeys = foreignKeys;
		Versions = versions;
	}
}
