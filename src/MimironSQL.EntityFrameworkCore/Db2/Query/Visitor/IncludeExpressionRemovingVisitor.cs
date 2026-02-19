using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;

using MimironSQL.EntityFrameworkCore.Infrastructure;

namespace MimironSQL.EntityFrameworkCore.Db2.Query.Visitor;

internal sealed class IncludeExpressionRemovingVisitor(ParameterExpression queryContextParameter) : ExpressionVisitor
{
    private readonly ParameterExpression _queryContextParameter = queryContextParameter;

    protected override Expression VisitExtension(Expression node)
    {
        // Correctness-first include execution:
        // EF Core may leave IncludeExpression nodes in the shaper, which are not reducible and cannot be compiled.
        // We rewrite them into explicit sync loads.
        // TODO: replace N+1 include loading with provider-native/batched execution.
        if (node is IncludeExpression includeExpression)
        {
            var entityExpression = Visit(includeExpression.EntityExpression);

            if (includeExpression.Navigation is not INavigationBase navigation)
                throw new NotSupportedException("MimironDb2 IncludeExpression without INavigationBase navigation is not supported.");

            var entityVariable = Expression.Variable(entityExpression.Type, "entity");
            var assign = Expression.Assign(entityVariable, entityExpression);

            var dbContextExpression = Expression.Property(_queryContextParameter, nameof(QueryContext.Context));

            var loadCall = Expression.Call(
                LoadNavigationMethodInfo,
                dbContextExpression,
                Expression.Convert(entityVariable, typeof(object)),
                Expression.Constant(navigation.Name),
                Expression.Constant(navigation.IsCollection));

            // IncludeExpression returns the entity.
            return Expression.Block(
                [entityVariable],
                assign,
                Expression.Condition(
                    Expression.Equal(Expression.Convert(entityVariable, typeof(object)), Expression.Constant(null, typeof(object))),
                    Expression.Empty(),
                    loadCall),
                entityVariable);
        }

        return base.VisitExtension(node);
    }

    private static readonly MethodInfo LoadNavigationMethodInfo = typeof(IncludeExpressionRemovingVisitor)
        .GetTypeInfo()
        .GetDeclaredMethod(nameof(LoadNavigation))!;

    private static void LoadNavigation(DbContext context, object entity, string navigationName, bool isCollection)
    {
        var entry = context.Entry(entity);

        if (isCollection)
        {
            var collection = entry.Collection(navigationName);
            if (collection.IsLoaded)
                return;

            // FK-array navigations are modeled as many-to-many with a virtual join entity.
            // During bootstrap, EF Core's ManyToManyLoader path generates SelectMany, which
            // our provider does not translate yet. Load these collections via the FK ID
            // array on the principal entity instead.
            if (collection.Metadata is ISkipNavigation skipNavigation)
            {
                var targetClrType = skipNavigation.TargetEntityType.ClrType;
                var loaded = (System.Collections.IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(targetClrType))!;

                var joinEntityType = skipNavigation.JoinEntityType;
                var fkArrayPropertyName = joinEntityType
                    .FindAnnotation(Db2ForeignKeyArrayModelRewriter.VirtualForeignKeyArrayPropertyAnnotation)
                    ?.Value as string;

                if (string.IsNullOrWhiteSpace(fkArrayPropertyName))
                {
                    throw new NotSupportedException(
                        $"MimironDb2 could not resolve FK array property for skip navigation '{skipNavigation.Name}'.");
                }

                var fkArray = context.Entry(entity).Property(fkArrayPropertyName).CurrentValue;
                if (fkArray is not Array ids)
                {
                    throw new NotSupportedException(
                        $"MimironDb2 expected FK array '{fkArrayPropertyName}' to be an array, but got '{fkArray?.GetType().FullName ?? "<null>"}'.");
                }

                for (var i = 0; i < ids.Length; i++)
                {
                    var id = ids.GetValue(i);
                    if (id is null)
                        continue;

                    var found = context.Find(targetClrType, id);
                    if (found is not null)
                        loaded.Add(found);
                }

                collection.CurrentValue = loaded;
                collection.IsLoaded = true;
                return;
            }

            collection.Load();
            return;
        }

        var reference = entry.Reference(navigationName);
        if (reference.IsLoaded)
            return;

        reference.Load();
    }
}
