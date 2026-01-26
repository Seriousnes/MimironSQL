using System.Reflection;

using MimironSQL.Db2.Schema;

namespace MimironSQL.Db2.Model;

public sealed class Db2EntityType(Type clrType, string tableName, Db2TableSchema schema, MemberInfo primaryKeyMember)
{
    public Type ClrType { get; } = clrType;
    public string TableName { get; } = tableName;

    public Db2TableSchema Schema { get; } = schema;
    public MemberInfo PrimaryKeyMember { get; } = primaryKeyMember;
}
