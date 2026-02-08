using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Extensions;

namespace MimironSQL.Db2.Model;

internal sealed partial class Db2EntityTypeBuilder<T>
{
    public Db2CollectionNavigationBuilder<T, TTarget> HasMany<TTarget>(Expression<Func<T, ICollection<TTarget>>> navigation)
        where TTarget : class?
    {
        ArgumentNullException.ThrowIfNull(navigation);

        var body = navigation.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            body = u.Operand;

        if (body is not MemberExpression { Member: PropertyInfo p } member)
            throw new NotSupportedException("HasMany only supports simple public property access (e.g., x => x.Children).");

        if (member.Expression != navigation.Parameters[0])
            throw new NotSupportedException("HasMany only supports direct member access on the root entity parameter.");

        if (p.GetMethod is not { IsPublic: true })
            throw new NotSupportedException($"Navigation property '{p.DeclaringType?.FullName}.{p.Name}' must have a public getter.");

        if (p.SetMethod is not { IsPublic: true })
            throw new NotSupportedException($"Navigation property '{p.DeclaringType?.FullName}.{p.Name}' must have a public setter.");

        var navMember = p;
        var navType = navMember.GetMemberType();

        var targetClrType = typeof(TTarget);

        var expectedNavType = typeof(ICollection<>).MakeGenericType(targetClrType);
        if (navType != expectedNavType)
        {
            throw new NotSupportedException(
                $"Navigation type mismatch: expected '{expectedNavType.FullName}' but found '{navType.FullName}'. " +
                $"Collection navigation properties must be declared as ICollection<{targetClrType.Name}>.");
        }

        var nav = new Db2CollectionNavigationMetadata(navMember, targetClrType);
        Metadata.CollectionNavigations.Add(nav);

        _modelBuilder.Entity<TTarget>();
        return new Db2CollectionNavigationBuilder<T, TTarget>(_modelBuilder, nav);
    }
}
