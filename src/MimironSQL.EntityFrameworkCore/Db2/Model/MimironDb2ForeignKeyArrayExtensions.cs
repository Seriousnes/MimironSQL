using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace MimironSQL.EntityFrameworkCore.Db2.Model;

/// <summary>
/// Fluent configuration extensions for configuring collection navigations that are backed by an array of foreign key ids
/// stored on the principal entity.
/// </summary>
public static class MimironDb2ForeignKeyArrayExtensions
{
    /// <summary>
    /// Marks a collection navigation as being backed by an <see cref="IEnumerable{T}"/> of foreign key ids on the
    /// principal entity.
    /// </summary>
    /// <typeparam name="TEntity">The principal entity CLR type.</typeparam>
    /// <typeparam name="TTarget">The dependent entity CLR type.</typeparam>
    /// <param name="builder">The relationship builder.</param>
    /// <param name="foreignKeyIds">A selector for the foreign key id array property (e.g., <c>x =&gt; x.ChildIds</c>).</param>
    /// <returns>The builder instance.</returns>
    public static ReferenceCollectionBuilder<TEntity, TTarget> HasForeignKeyArray<TEntity, TTarget>(
        this ReferenceCollectionBuilder<TEntity, TTarget> builder,
        Expression<Func<TEntity, IEnumerable<int>>> foreignKeyIds)
        where TEntity : class
        where TTarget : class
        => builder.HasForeignKeyArray<TEntity, TTarget, int>(foreignKeyIds);

    /// <summary>
    /// Marks a collection navigation as being backed by an <see cref="IEnumerable{T}"/> of foreign key ids on the
    /// principal entity.
    /// </summary>
    /// <typeparam name="TEntity">The principal entity CLR type.</typeparam>
    /// <typeparam name="TTarget">The dependent entity CLR type.</typeparam>
    /// <typeparam name="TKey">The key element type.</typeparam>
    /// <param name="builder">The relationship builder.</param>
    /// <param name="foreignKeyIds">A selector for the foreign key id array property (e.g., <c>x =&gt; x.ChildIds</c>).</param>
    /// <returns>The builder instance.</returns>
    public static ReferenceCollectionBuilder<TEntity, TTarget> HasForeignKeyArray<TEntity, TTarget, TKey>(
        this ReferenceCollectionBuilder<TEntity, TTarget> builder,
        Expression<Func<TEntity, IEnumerable<TKey>>> foreignKeyIds)
        where TEntity : class
        where TTarget : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(foreignKeyIds);

        var member = GetForeignKeyArrayMember(foreignKeyIds);
        Db2ForeignKeyArrayAnnotations.SetForeignKeyArrayPropertyName(GetPrincipalToDependentNavigation(builder), member.Name);
        return builder;

        static IMutableNavigation GetPrincipalToDependentNavigation(object relationshipBuilder)
        {
            var fk = GetForeignKeyMetadata(relationshipBuilder);

            const BindingFlags flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

            if (fk.GetType().GetProperty("PrincipalToDependent", flags)?.GetValue(fk) is IMutableNavigation principalToDependent)
                return principalToDependent;

            throw new NotSupportedException("Unable to access EF Core principal-to-dependent navigation metadata for HasForeignKeyArray.");

            static object GetForeignKeyMetadata(object builder)
            {
                const BindingFlags f = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

                if (builder.GetType().GetProperty("Metadata", f)?.GetValue(builder) is IMutableForeignKey direct)
                    return direct;

                foreach (var iface in builder.GetType().GetInterfaces())
                {
                    if (!iface.IsGenericType)
                        continue;

                    if (iface.GetGenericTypeDefinition() != typeof(IInfrastructure<>))
                        continue;

                    var instance = iface.GetProperty(nameof(IInfrastructure<object>.Instance))?.GetValue(builder);
                    if (instance is null)
                        continue;

                    if (instance.GetType().GetProperty("Metadata", f)?.GetValue(instance) is IMutableForeignKey fk)
                        return fk;
                }

                throw new NotSupportedException("Unable to access EF Core foreign key metadata for HasForeignKeyArray.");
            }
        }
    }

    /// <summary>
    /// Marks a collection navigation as being backed by an <see cref="IEnumerable{T}"/> of foreign key ids on the
    /// declaring entity.
    /// </summary>
    /// <typeparam name="TEntity">The declaring entity CLR type.</typeparam>
    /// <typeparam name="TTarget">The target entity CLR type.</typeparam>
    /// <param name="builder">The navigation builder.</param>
    /// <param name="foreignKeyIds">A selector for the foreign key id array property (e.g., <c>x =&gt; x.ChildIds</c>).</param>
    /// <returns>The builder instance.</returns>
    public static CollectionNavigationBuilder<TEntity, TTarget> HasForeignKeyArray<TEntity, TTarget>(
        this CollectionNavigationBuilder<TEntity, TTarget> builder,
        Expression<Func<TEntity, IEnumerable<int>>> foreignKeyIds)
        where TEntity : class
        where TTarget : class
        => builder.HasForeignKeyArray<TEntity, TTarget, int>(foreignKeyIds);

    /// <summary>
    /// Marks a collection navigation as being backed by an <see cref="IEnumerable{T}"/> of foreign key ids on the
    /// declaring entity.
    /// </summary>
    /// <typeparam name="TEntity">The declaring entity CLR type.</typeparam>
    /// <typeparam name="TTarget">The target entity CLR type.</typeparam>
    /// <typeparam name="TKey">The key element type.</typeparam>
    /// <param name="builder">The navigation builder.</param>
    /// <param name="foreignKeyIds">A selector for the foreign key id array property (e.g., <c>x =&gt; x.ChildIds</c>).</param>
    /// <returns>The builder instance.</returns>
    public static CollectionNavigationBuilder<TEntity, TTarget> HasForeignKeyArray<TEntity, TTarget, TKey>(
        this CollectionNavigationBuilder<TEntity, TTarget> builder,
        Expression<Func<TEntity, IEnumerable<TKey>>> foreignKeyIds)
        where TEntity : class
        where TTarget : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(foreignKeyIds);

        var member = GetForeignKeyArrayMember(foreignKeyIds);
        Db2ForeignKeyArrayAnnotations.SetForeignKeyArrayPropertyName(GetNavigationMetadata(builder), member.Name);
        return builder;

        static IMutableNavigation GetNavigationMetadata(object navigationBuilder)
        {
            // EF Core's fluent builder APIs have shifted across major versions.
            // Prefer direct 'Metadata' if present, otherwise fall back to the IInfrastructure<> instance.
            var type = navigationBuilder.GetType();

            if (TryGetNavigationFromObject(navigationBuilder, out var direct))
                return direct;

            foreach (var iface in type.GetInterfaces())
            {
                if (!iface.IsGenericType)
                    continue;

                if (iface.GetGenericTypeDefinition() != typeof(IInfrastructure<>))
                    continue;

                var instance = iface.GetProperty(nameof(IInfrastructure<object>.Instance))?.GetValue(navigationBuilder);
                if (instance is null)
                    continue;

                if (TryGetNavigationFromObject(instance, out var nav))
                    return nav;
            }

            throw new NotSupportedException($"Unable to access EF Core navigation metadata for HasForeignKeyArray. Builder type: '{type.FullName}'.");
        }

        static bool TryGetNavigationFromObject(object value, out IMutableNavigation navigation)
        {
            if (value is IMutableNavigation n)
            {
                navigation = n;
                return true;
            }

            var type = value.GetType();
            const BindingFlags Flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

            if (type.GetProperty("Metadata", Flags)?.GetValue(value) is IMutableNavigation direct)
            {
                navigation = direct;
                return true;
            }

            foreach (var p in type.GetProperties(Flags))
            {
                if (!typeof(IMutableNavigation).IsAssignableFrom(p.PropertyType))
                    continue;

                if (p.GetIndexParameters().Length != 0)
                    continue;

                if (p.GetValue(value) is IMutableNavigation propNav)
                {
                    navigation = propNav;
                    return true;
                }
            }

            foreach (var f in type.GetFields(Flags))
            {
                if (!typeof(IMutableNavigation).IsAssignableFrom(f.FieldType))
                    continue;

                if (f.GetValue(value) is IMutableNavigation fieldNav)
                {
                    navigation = fieldNav;
                    return true;
                }
            }

            navigation = null!;
            return false;
        }

    }

    private static PropertyInfo GetForeignKeyArrayMember(LambdaExpression expression)
    {
        if (expression.Parameters is not { Count: 1 })
            throw new NotSupportedException("FK array selector must have exactly one parameter.");

        var body = expression.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            body = u.Operand;

        if (body is not MemberExpression { Member: PropertyInfo p } member)
            throw new NotSupportedException("FK array selector only supports simple public property access (e.g., x => x.ChildIds).");

        if (member.Expression != expression.Parameters[0])
            throw new NotSupportedException("FK array selector only supports direct member access on the root entity parameter.");

        return p.GetMethod switch
        {
            not { IsPublic: true } => throw new NotSupportedException($"FK array property '{p.DeclaringType?.FullName}.{p.Name}' must have a public getter."),
            _ => p,
        };
    }
}
