using System;

namespace MimironSQL.Db2.Query;

[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public sealed class Db2TableNameAttribute(string tableName) : Attribute
{
    public string TableName { get; } = tableName;
}
