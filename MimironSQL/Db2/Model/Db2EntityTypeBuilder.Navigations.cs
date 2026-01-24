using System.Linq.Expressions;
using System.Reflection;

namespace MimironSQL.Db2.Model;

public sealed partial class Db2EntityTypeBuilder<T>
{
    public Db2ReferenceNavigationBuilder<T, TTarget> HasOne<TTarget>(Expression<Func<T, TTarget?>> navigation)
        where TTarget : class
    {
        ArgumentNullException.ThrowIfNull(navigation);

        var body = navigation.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            body = u.Operand;

        if (body is not MemberExpression { Member: PropertyInfo or FieldInfo } member)
            throw new NotSupportedException("HasOne only supports simple member access (e.g., x => x.Parent). ");

        if (member.Expression != navigation.Parameters[0])
            throw new NotSupportedException("HasOne only supports direct member access on the root entity parameter.");

        var navMember = member.Member;
        var navType = GetMemberType(navMember);

        var targetClrType = typeof(TTarget);
        if (!targetClrType.IsAssignableFrom(navType))
            throw new NotSupportedException($"Navigation type mismatch: expected {targetClrType.FullName} but found {navType.FullName}.");

        var nav = new Db2NavigationMetadata(navMember, targetClrType);
        Metadata.Navigations.Add(nav);

        return new Db2ReferenceNavigationBuilder<T, TTarget>(_modelBuilder, nav);
    }

    private static Type GetMemberType(MemberInfo member)
        => member switch
        {
            PropertyInfo p => p.PropertyType,
            FieldInfo f => f.FieldType,
            _ => throw new InvalidOperationException($"Unexpected member type: {member.GetType().FullName}"),
        };
}
