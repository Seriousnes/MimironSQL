namespace MimironSQL.Db2.Query;

[AttributeUsage(AttributeTargets.Struct | AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class Db2TableNameAttribute(string tableName) : Attribute
{
    public string TableName { get; } = tableName;
}
