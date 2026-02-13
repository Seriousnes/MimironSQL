using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore.Metadata.Builders;

using MimironSQL.Db2;

namespace MimironSQL.EntityFrameworkCore.Db2.Model;

/// <summary>
/// Fluent configuration extensions for configuring one-to-one relationships where the dependent primary key is also the
/// foreign key to the principal primary key ("shared primary key").
/// </summary>
public static class MimironDb2SharedPrimaryKeyOneToOneExtensions
{
    /// <summary>
    /// Configures a one-to-one relationship where the dependent key is both the primary key and the foreign key to the
    /// principal key ("shared primary key"), using an <c>Id</c> property on both entity types.
    /// </summary>
    /// <param name="builder">The principal entity type builder.</param>
    /// <param name="principalNavigation">The principal-to-dependent navigation property selector.</param>
    /// <param name="dependentNavigation">The dependent-to-principal navigation property selector.</param>
    /// <param name="required">Whether the dependent is required.</param>
    /// <returns>The relationship builder.</returns>
    public static ReferenceReferenceBuilder<TPrincipal, TDependent> HasSharedPrimaryKey<TPrincipal, TDependent>(
        this EntityTypeBuilder<TPrincipal> builder,
        Expression<Func<TPrincipal, TDependent?>> principalNavigation,
        Expression<Func<TDependent, TPrincipal?>> dependentNavigation,
        bool required = true)
        where TPrincipal : class
        where TDependent : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(principalNavigation);
        ArgumentNullException.ThrowIfNull(dependentNavigation);

        _ = GetMember(principalNavigation);
        _ = GetMember(dependentNavigation);

        var principalKey = typeof(TPrincipal).GetProperty(nameof(Db2Entity<>.Id), BindingFlags.Instance | BindingFlags.Public);
        if (principalKey is null || principalKey.GetMethod is not { IsPublic: true })
            throw new NotSupportedException($"Shared primary key one-to-one requires a public '{nameof(Db2Entity<>.Id)}' property on principal type '{typeof(TPrincipal).FullName}'.");

        var dependentKey = typeof(TDependent).GetProperty(nameof(Db2Entity<>.Id), BindingFlags.Instance | BindingFlags.Public);
        if (dependentKey is null || dependentKey.GetMethod is not { IsPublic: true })
            throw new NotSupportedException($"Shared primary key one-to-one requires a public '{nameof(Db2Entity<>.Id)}' property on dependent type '{typeof(TDependent).FullName}'.");

        if (principalKey.PropertyType != dependentKey.PropertyType)
        {
            throw new NotSupportedException(
                $"Shared primary key one-to-one requires principal and dependent '{nameof(Db2Entity<>.Id)}' properties to have the same type. " +
                $"Principal '{typeof(TPrincipal).FullName}.{nameof(Db2Entity<>.Id)}' is '{principalKey.PropertyType.FullName}', dependent '{typeof(TDependent).FullName}.{nameof(Db2Entity<>.Id)}' is '{dependentKey.PropertyType.FullName}'.");
        }

        return builder
            .HasOne(principalNavigation)
            .WithOne(dependentNavigation)
            .HasForeignKey<TDependent>(nameof(Db2Entity<>.Id))
            .HasPrincipalKey<TPrincipal>(nameof(Db2Entity<>.Id))
            .IsRequired(required);
    }

    private static PropertyInfo GetMember(LambdaExpression expression)
    {
        if (expression.Parameters is not { Count: 1 })
            throw new NotSupportedException("Key selector must have exactly one parameter.");

        var body = expression.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            body = u.Operand;

        if (body is not MemberExpression { Member: PropertyInfo p })
            throw new NotSupportedException("Key selector must target a property (e.g., x => x.Id).");

        if (p.GetMethod is not { IsPublic: true })
            throw new NotSupportedException($"Key member '{p.DeclaringType?.FullName}.{p.Name}' must have a public getter.");

        return p;
    }
}
