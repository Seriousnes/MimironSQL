namespace MimironSQL.Db2.Model;

public sealed class Db2EntityType(Type clrType, string tableName)
{
    public Type ClrType { get; } = clrType;
    public string TableName { get; } = tableName;
}
