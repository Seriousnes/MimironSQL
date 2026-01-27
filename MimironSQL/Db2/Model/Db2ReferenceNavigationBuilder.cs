using System.Linq.Expressions;
using System.Reflection;

namespace MimironSQL.Db2.Model;

public sealed class Db2ReferenceNavigationBuilder<TSource, TTarget>(Db2ModelBuilder modelBuilder, Db2NavigationMetadata metadata)
{
    private readonly Db2ModelBuilder _modelBuilder = modelBuilder;
    private readonly Db2NavigationMetadata _metadata = metadata;

    public Db2ReferenceNavigationBuilder<TSource, TTarget> WithSharedPrimaryKey<TKey>(
        Expression<Func<TSource, TKey>> sourceKey,
        Expression<Func<TTarget, TKey>> targetKey)
    {
        ArgumentNullException.ThrowIfNull(sourceKey);
        ArgumentNullException.ThrowIfNull(targetKey);

        _metadata.Kind = Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne;
        _metadata.SourceKeyMember = GetMember(sourceKey);
        _metadata.TargetKeyMember = GetMember(targetKey);

        _modelBuilder.Entity(typeof(TTarget));
        return this;

        static MemberInfo GetMember(LambdaExpression expression)
        {
            if (expression.Parameters is not { Count: 1 })
                throw new NotSupportedException("Key selector must have exactly one parameter.");

            var body = expression.Body;
            if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                body = u.Operand;

            if (body is not MemberExpression { Member: PropertyInfo or FieldInfo } member)
                throw new NotSupportedException("Key selector only supports simple member access (e.g., x => x.Id). ");

            if (member.Expression != expression.Parameters[0])
                throw new NotSupportedException("Key selector only supports direct member access on the root entity parameter.");

            return member.Member;
        }
    }

    public Db2ReferenceNavigationBuilder<TSource, TTarget> OverridesSchema()
    {
        _metadata.OverridesSchema = true;
        return this;
    }

    public Db2ReferenceNavigationBuilder<TSource, TTarget> WithForeignKey<TKey>(
        Expression<Func<TSource, TKey>> foreignKey)
    {
        ArgumentNullException.ThrowIfNull(foreignKey);

        _metadata.Kind = Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey;
        _metadata.SourceKeyMember = GetMember(foreignKey);

        _modelBuilder.Entity(typeof(TTarget));
        return this;

        static MemberInfo GetMember(LambdaExpression expression)
        {
            if (expression.Parameters is not { Count: 1 })
                throw new NotSupportedException("FK selector must have exactly one parameter.");

            var body = expression.Body;
            if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                body = u.Operand;

            if (body is not MemberExpression { Member: PropertyInfo or FieldInfo } member)
                throw new NotSupportedException("FK selector only supports simple member access (e.g., x => x.ParentId). ");

            if (member.Expression != expression.Parameters[0])
                throw new NotSupportedException("FK selector only supports direct member access on the root entity parameter.");

            return member.Member;
        }
    }

    public Db2ReferenceNavigationBuilder<TSource, TTarget> HasPrincipalKey<TKey>(
        Expression<Func<TTarget, TKey>> principalKey)
    {
        ArgumentNullException.ThrowIfNull(principalKey);

        _metadata.TargetKeyMember = GetMember(principalKey);

        _modelBuilder.Entity(typeof(TTarget));
        return this;

        static MemberInfo GetMember(LambdaExpression expression)
        {
            if (expression.Parameters is not { Count: 1 })
                throw new NotSupportedException("Principal key selector must have exactly one parameter.");

            var body = expression.Body;
            if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                body = u.Operand;

            if (body is not MemberExpression { Member: PropertyInfo or FieldInfo } member)
                throw new NotSupportedException("Principal key selector only supports simple member access (e.g., x => x.Id). ");

            if (member.Expression != expression.Parameters[0])
                throw new NotSupportedException("Principal key selector only supports direct member access on the root entity parameter.");

            return member.Member;
        }
    }
}
