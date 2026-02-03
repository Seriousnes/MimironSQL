namespace MimironSQL.DbContextGenerator;

internal sealed class ForeignKeySpec(string columnName, string targetTableName, string targetColumnName)
{
    public string ColumnName { get; } = columnName;
    public string TargetTableName { get; } = targetTableName;
    public string TargetColumnName { get; } = targetColumnName;
}
