namespace MimironSQL.DbContextGenerator;

internal sealed class ForeignKeySpec
{
	public string ColumnName { get; }
	public string TargetTableName { get; }
	public string TargetColumnName { get; }

	public ForeignKeySpec(string columnName, string targetTableName, string targetColumnName)
	{
		ColumnName = columnName;
		TargetTableName = targetTableName;
		TargetColumnName = targetColumnName;
	}
}
