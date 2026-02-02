namespace CASC.Net.Generators;

internal sealed record ForeignKeySpec(string ColumnName, string TargetTableName, string TargetColumnName);
