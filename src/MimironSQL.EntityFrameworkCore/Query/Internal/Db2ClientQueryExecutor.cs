using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

using MimironSQL.EntityFrameworkCore.Query.Internal.Expressions;
using MimironSQL.EntityFrameworkCore.Query.Internal.Visitor;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

internal static class Db2ClientQueryExecutor
{
    internal static IEnumerable<TResult> Query<TResult>(
        QueryContext queryContext,
        Db2QueryExpression queryExpression,
        int remaining,
        IEnumerable<ValueBuffer> valueBuffers,
        Func<QueryContext, ValueBuffer, TResult> shaper,
        Func<QueryContext, ValueBuffer, object> entityShaper,
        IncludePlan[] includePlans)
    {
        queryContext.InitializeStateManager(standAlone: false);

        var entityType = queryExpression.EntityType;

        var entityPredicates = new List<Func<QueryContext, object, bool>>();
        var resultPredicates = new List<Func<QueryContext, object, bool>>();

        if (queryExpression.Predicates.Count > 0)
        {
            var resultClrType = typeof(TResult);
            foreach (var predicate in queryExpression.Predicates)
            {
                var parameterType = predicate.Parameters[0].Type;

                if (parameterType == entityType.ClrType
                    || Db2RowPredicateCompiler.TryGetTransparentIdentifierTypes(parameterType, out _, out _))
                {
                    entityPredicates.Add(CompilePredicate(entityType, predicate));
                    continue;
                }

                if (parameterType == resultClrType
                    || parameterType == typeof(object)
                    || parameterType.IsAssignableFrom(resultClrType))
                {
                    resultPredicates.Add(CompileResultPredicate(predicate));
                    continue;
                }

                throw new NotSupportedException(
                    $"MimironDb2 cannot apply predicate with parameter type '{parameterType.FullName}' to shaper result type '{resultClrType.FullName}'.");
            }
        }

        var results = new List<TResult>();

        foreach (var valueBuffer in valueBuffers)
        {
            if (remaining == 0)
                break;

            if (entityPredicates.Count > 0)
            {
                object? entity;
                try
                {
                    entity = entityShaper(queryContext, valueBuffer);
                }
                catch (InvalidCastException ex)
                {
                    throw CreateShaperDebugException(queryExpression, valueBuffer, ex);
                }

                if (entity is null)
                    continue;

                if (entityPredicates.Any(p => !p(queryContext, entity)))
                    continue;
            }

            TResult result;
            try
            {
                result = shaper(queryContext, valueBuffer);
            }
            catch (InvalidCastException ex)
            {
                throw CreateShaperDebugException(queryExpression, valueBuffer, ex);
            }

            if (resultPredicates.Count > 0)
            {
                if (result is null)
                    continue;

                var boxed = (object)result;

                if (resultPredicates.Any(p => !p(queryContext, boxed)))
                    continue;
            }

            results.Add(result);
            if (remaining > 0)
                remaining--;
        }

        if (includePlans is { Length: > 0 } && results.Count > 0)
            new Db2IncludeExecutor().ExecuteIncludes(queryContext, includePlans, results);

        return results;
    }

    private static Exception CreateShaperDebugException(Db2QueryExpression queryExpression, ValueBuffer valueBuffer, InvalidCastException inner)
    {
        var sb = new System.Text.StringBuilder();
        sb.AppendLine("MimironDb2 shaper InvalidCastException (bootstrap debug):");
        sb.AppendLine($"- EntityType: {queryExpression.EntityType.DisplayName()}");
        sb.AppendLine($"- Joins: {queryExpression.Joins.Count}");
        if (queryExpression.Joins.Count > 0)
        {
            foreach (var (joinOp, innerQuery, outerKeySelector, innerKeySelector) in queryExpression.Joins)
            {
                sb.AppendLine($"  - {joinOp} inner={innerQuery.EntityType.DisplayName()} outerKey={outerKeySelector.Body} innerKey={innerKeySelector.Body}");
            }
        }

        var count = valueBuffer.Count;
        sb.AppendLine($"- ValueBuffer.Count: {count}");

        var take = Math.Min(count, 48);
        for (var i = 0; i < take; i++)
        {
            var v = valueBuffer[i];
            var typeName = v?.GetType().FullName ?? "<null>";
            var valueString = v is null
                ? "<null>"
                : v is string s ? $"\"{s}\""
                : v is Array a ? $"{a.GetType().GetElementType()?.Name}[] (len={a.Length})"
                : v.ToString() ?? "<null>";

            sb.AppendLine($"  [{i}] {typeName} = {valueString}");
        }

        return new InvalidOperationException(sb.ToString(), inner);
    }

    private static Func<QueryContext, object, bool> CompilePredicate(IEntityType entityType, LambdaExpression predicate)
    {
        ArgumentNullException.ThrowIfNull(entityType);
        ArgumentNullException.ThrowIfNull(predicate);

        var entityClrType = entityType.ClrType;
        var expectedParameterType = predicate.Parameters[0].Type;

        var queryContextParameter = Expression.Parameter(typeof(QueryContext), "qc");
        var entityParameter = Expression.Parameter(entityClrType, "e");

        Expression rewrittenBody;
        if (Db2RowPredicateCompiler.TryGetTransparentIdentifierTypes(expectedParameterType, out var outerType, out var innerType))
        {
            if (outerType != entityClrType)
                throw new NotSupportedException($"MimironDb2 cannot apply TransparentIdentifier predicate with outer '{outerType.FullName}' to entity '{entityClrType.FullName}'.");

            var matching = entityType.GetNavigations()
                .Where(n => !n.IsCollection && n.TargetEntityType.ClrType == innerType)
                .ToArray();

            if (matching.Length == 0)
                throw new NotSupportedException(
                    $"MimironDb2 cannot rewrite TransparentIdentifier predicate: no reference navigation from '{entityClrType.FullName}' to '{innerType.FullName}' was found.");

            if (matching.Length > 1)
                throw new NotSupportedException(
                    $"MimironDb2 cannot rewrite TransparentIdentifier predicate: multiple reference navigations from '{entityClrType.FullName}' to '{innerType.FullName}' exist.");

            var navigation = matching[0];

            if (navigation.PropertyInfo is null)
                throw new NotSupportedException(
                    $"MimironDb2 cannot rewrite TransparentIdentifier predicate: navigation '{navigation.Name}' has no PropertyInfo.");

            var transparentParameter = predicate.Parameters[0];
            var innerAccess = Expression.Property(entityParameter, navigation.PropertyInfo);

            rewrittenBody = new TransparentIdentifierRewritingVisitor(transparentParameter, entityParameter, innerAccess)
                .Visit(predicate.Body);

            if (ParameterSearchVisitor.Contains(rewrittenBody, transparentParameter))
                throw new NotSupportedException(
                    "MimironDb2 cannot rewrite TransparentIdentifier predicate: unsupported usage of the transparent identifier parameter.");
        }
        else
        {
            if (!expectedParameterType.IsAssignableFrom(entityClrType))
                throw new NotSupportedException(
                    $"MimironDb2 cannot apply predicate expecting '{expectedParameterType.FullName}' to entity '{entityClrType.FullName}'.");

            rewrittenBody = new ParameterReplaceVisitor(predicate.Parameters[0], entityParameter).Visit(predicate.Body);
        }

        rewrittenBody = new QueryParameterRemovingVisitor(queryContextParameter).Visit(rewrittenBody);
        rewrittenBody = new CorrelatedNavigationRemovingVisitor(queryContextParameter).Visit(rewrittenBody);
        rewrittenBody = new EfPropertyRemovingVisitor(queryContextParameter).Visit(rewrittenBody);

        var boolBody = rewrittenBody.Type == typeof(bool)
            ? rewrittenBody
            : Expression.Convert(rewrittenBody, typeof(bool));

        var typedDelegateType = typeof(Func<,,>).MakeGenericType(typeof(QueryContext), entityClrType, typeof(bool));
        var typedLambda = Expression.Lambda(typedDelegateType, boolBody, queryContextParameter, entityParameter);

        object typedPredicate;
        try
        {
            typedPredicate = typedLambda.Compile();
        }
        catch (ArgumentException ex)
        {
            throw new NotSupportedException(
                "MimironDb2 failed to compile an EF Core predicate during bootstrap execution. "
                + "This usually indicates the predicate still contains non-reducible EF Core extension nodes (e.g., navigation expansion artifacts). "
                + $"EntityType='{entityType.DisplayName()}'. ExpectedParameterType='{expectedParameterType.FullName}'. "
                + $"OriginalBody='{predicate.Body}'. RewrittenBody='{rewrittenBody}'.",
                ex);
        }

        var boxedEntity = Expression.Parameter(typeof(object), "entity");
        var invoke = Expression.Invoke(
            Expression.Constant(typedPredicate, typedDelegateType),
            queryContextParameter,
            Expression.Convert(boxedEntity, entityClrType));
        return Expression.Lambda<Func<QueryContext, object, bool>>(invoke, queryContextParameter, boxedEntity).Compile();
    }

    private static Func<QueryContext, object, bool> CompileResultPredicate(LambdaExpression predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        if (predicate.Parameters.Count != 1)
            throw new NotSupportedException("MimironDb2 only supports single-parameter predicates during bootstrap execution.");

        var queryContextParameter = Expression.Parameter(typeof(QueryContext), "qc");
        var parameterType = predicate.Parameters[0].Type;
        var boxedParameter = Expression.Parameter(typeof(object), "r");
        var typedParameter = Expression.Convert(boxedParameter, parameterType);

        var rewrittenBody = new ParameterReplaceVisitor(predicate.Parameters[0], typedParameter).Visit(predicate.Body);
        rewrittenBody = new QueryParameterRemovingVisitor(queryContextParameter).Visit(rewrittenBody!);
        rewrittenBody = new EfPropertyRemovingVisitor(queryContextParameter).Visit(rewrittenBody!);

        var lambda = Expression.Lambda<Func<QueryContext, object, bool>>(
            rewrittenBody!,
            queryContextParameter,
            boxedParameter);

        return lambda.Compile();
    }
}
