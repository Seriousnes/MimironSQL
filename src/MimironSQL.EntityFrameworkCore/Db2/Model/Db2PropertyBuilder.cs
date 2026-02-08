using System.ComponentModel.DataAnnotations.Schema;
using System.Linq.Expressions;
using System.Reflection;

namespace MimironSQL.Db2.Model;

internal sealed class Db2PropertyBuilder<T>
{
    private readonly Db2EntityTypeMetadata _metadata;
    private readonly PropertyInfo _property;

    internal Db2PropertyBuilder(Db2EntityTypeMetadata metadata, PropertyInfo property)
    {
        _metadata = metadata;
        _property = property;
    }

    public Db2PropertyBuilder<T> HasColumnName(string columnName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(columnName);

        if (_property.Name.Equals("Id", StringComparison.OrdinalIgnoreCase))
            throw new NotSupportedException($"Primary key property '{_metadata.ClrType.FullName}.{_property.Name}' cannot configure column mapping via HasColumnName().");

        if (_property.GetCustomAttribute<ColumnAttribute>(inherit: false) is not null)
        {
            throw new NotSupportedException(
                $"Property '{_metadata.ClrType.FullName}.{_property.Name}' has a [Column] attribute and cannot also be configured with HasColumnName().");
        }

        _metadata.ColumnNameMappings[_property.Name] = columnName;
        return this;
    }

    internal static PropertyInfo ResolveProperty<TProperty>(Expression<Func<T, TProperty>> property)
    {
        ArgumentNullException.ThrowIfNull(property);

        if (property.Parameters is not { Count: 1 })
            throw new NotSupportedException("Property selector must have exactly one parameter.");

        var body = property.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            body = u.Operand;

        if (body is not MemberExpression { Member: PropertyInfo p } member)
            throw new NotSupportedException("Property selector only supports simple public property access (e.g., x => x.Name). ");

        if (member.Expression != property.Parameters[0])
            throw new NotSupportedException("Property selector only supports direct member access on the root entity parameter.");

        return p.GetMethod switch
        {
            not { IsPublic: true } => throw new NotSupportedException($"Property '{p.DeclaringType?.FullName}.{p.Name}' must have a public getter for column mapping."),
            _ => p,
        };
    }
}
