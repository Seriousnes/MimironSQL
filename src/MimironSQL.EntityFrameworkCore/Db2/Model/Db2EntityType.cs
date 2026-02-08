using System.Reflection;
using System.ComponentModel.DataAnnotations.Schema;

using MimironSQL.EntityFrameworkCore.Db2.Schema;

namespace MimironSQL.EntityFrameworkCore.Db2.Model;

internal sealed class Db2EntityType(
    Type clrType,
    string tableName,
    Db2TableSchema schema,
    MemberInfo primaryKeyMember,
    Db2FieldSchema primaryKeyFieldSchema,
    IReadOnlyDictionary<string, string> columnNameMappings)
{
    private readonly IReadOnlyDictionary<string, string> _columnNameMappings = columnNameMappings;

    internal IReadOnlyDictionary<string, string> ColumnNameMappings => _columnNameMappings;

    public Type ClrType { get; } = clrType;
    public string TableName { get; } = tableName;

    public Db2TableSchema Schema { get; } = schema;
    public MemberInfo PrimaryKeyMember { get; } = primaryKeyMember;
    public Db2FieldSchema PrimaryKeyFieldSchema { get; } = primaryKeyFieldSchema;

    internal bool TryResolveFieldSchema(MemberInfo member, out Db2FieldSchema fieldSchema)
    {
        if (member == PrimaryKeyMember)
        {
            fieldSchema = PrimaryKeyFieldSchema;
            return true;
        }

        if (member is not PropertyInfo { GetMethod.IsPublic: true } p)
        {
            fieldSchema = default;
            return false;
        }

        var columnName = ResolveColumnName(p);
        return Schema.TryGetFieldCaseInsensitive(columnName, out fieldSchema);
    }

    internal Db2FieldSchema ResolveFieldSchema(MemberInfo member, string context)
    {
        if (!TryResolveFieldSchema(member, out var fieldSchema))
            throw new NotSupportedException($"Field mapping for member '{ClrType.FullName}.{member.Name}' could not be resolved in schema '{Schema.TableName}' ({context}).");

        return fieldSchema;
    }

    private string ResolveColumnName(PropertyInfo property)
    {
        if (_columnNameMappings.TryGetValue(property.Name, out var configured))
            return configured;

        var attr = property.GetCustomAttribute<ColumnAttribute>(inherit: false);
        return attr switch
        {
            not null when !string.IsNullOrWhiteSpace(attr.Name) => attr.Name,
            _ => property.Name,
        };
    }

    internal Db2EntityType WithSchema(string tableName, Db2TableSchema schema)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(schema);

        var idFieldSchema = schema.Fields.FirstOrDefault(f => f.IsId);
        if (idFieldSchema.Equals(default) || string.IsNullOrWhiteSpace(idFieldSchema.Name))
            throw new NotSupportedException($"Id field was not found in schema '{schema.TableName}'.");

        return new Db2EntityType(
            clrType: ClrType,
            tableName: tableName,
            schema: schema,
            primaryKeyMember: PrimaryKeyMember,
            primaryKeyFieldSchema: idFieldSchema,
            columnNameMappings: _columnNameMappings);
    }
}
