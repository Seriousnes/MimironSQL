using System.Collections.Immutable;

namespace MimironSQL.DbContextGenerator;

internal sealed class ParsedTable
{
	public string TableName { get; }
	public ImmutableArray<ColumnSpec> Columns { get; }
	public ImmutableArray<KeySpec> Keys { get; }
	public ImmutableArray<ForeignKeySpec> ForeignKeys { get; }
	public ImmutableArray<DbdVersionDefinition> Versions { get; }

	public ParsedTable(
		string tableName,
		ImmutableArray<ColumnSpec> columns,
		ImmutableArray<KeySpec> keys,
		ImmutableArray<ForeignKeySpec> foreignKeys,
		ImmutableArray<DbdVersionDefinition> versions)
	{
		TableName = tableName;
		Columns = columns;
		Keys = keys;
		ForeignKeys = foreignKeys;
		Versions = versions;
	}
}

internal sealed class DbdVersionDefinition
{
	public ImmutableArray<uint> LayoutHashes { get; }
	public ImmutableArray<DbdBuildSpec> Builds { get; }
	public ImmutableArray<DbdPhysicalColumnSpec> Columns { get; }

	public DbdVersionDefinition(
		ImmutableArray<uint> layoutHashes,
		ImmutableArray<DbdBuildSpec> builds,
		ImmutableArray<DbdPhysicalColumnSpec> columns)
	{
		LayoutHashes = layoutHashes;
		Builds = builds;
		Columns = columns;
	}
}

internal abstract class DbdBuildSpec
{
	public sealed class Exact : DbdBuildSpec
	{
		public string Version { get; }

		public Exact(string version)
		{
			Version = version;
		}
	}

	public sealed class Range : DbdBuildSpec
	{
		public string From { get; }
		public string To { get; }

		public Range(string from, string to)
		{
			From = from;
			To = to;
		}
	}
}

internal sealed class DbdPhysicalColumnSpec
{
	public string ColumnName { get; }
	public int? ArrayLength { get; }
	public bool IsId { get; }
	public bool IsRelation { get; }
	public bool IsNonInline { get; }

	public DbdPhysicalColumnSpec(
		string columnName,
		int? arrayLength,
		bool isId,
		bool isRelation,
		bool isNonInline)
	{
		ColumnName = columnName;
		ArrayLength = arrayLength;
		IsId = isId;
		IsRelation = isRelation;
		IsNonInline = isNonInline;
	}
}
