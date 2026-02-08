using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Extensions;

namespace MimironSQL.Db2.Model;

internal sealed partial class Db2EntityTypeBuilder<T>
{
    public Db2ReferenceNavigationBuilder<T, TTarget> HasOne<TTarget>(Expression<Func<T, TTarget?>> navigation)
        where TTarget : class
    {
        ArgumentNullException.ThrowIfNull(navigation);

        var body = navigation.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            body = u.Operand;

        if (body is not MemberExpression { Member: PropertyInfo p } member)
            throw new NotSupportedException("HasOne only supports simple public property access (e.g., x => x.Parent). ");

        if (member.Expression != navigation.Parameters[0])
            throw new NotSupportedException("HasOne only supports direct member access on the root entity parameter.");

        if (p.GetMethod is not { IsPublic: true })
            throw new NotSupportedException($"Navigation property '{p.DeclaringType?.FullName}.{p.Name}' must have a public getter.");

        var navMember = p;
        var navType = navMember.GetMemberType();

        var targetClrType = typeof(TTarget);
        if (!targetClrType.IsAssignableFrom(navType))
            throw new NotSupportedException($"Navigation type mismatch: expected {targetClrType.FullName} but found {navType.FullName}.");

        var nav = new Db2NavigationMetadata(navMember, targetClrType);
        Metadata.Navigations.Add(nav);

        return new Db2ReferenceNavigationBuilder<T, TTarget>(_modelBuilder, nav);
    }

}
