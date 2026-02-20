using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

internal sealed class MimironDb2QueryableMethodTranslatingExpressionVisitor(
    QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
    QueryCompilationContext queryCompilationContext,
    bool subquery)
    : QueryableMethodTranslatingExpressionVisitor(dependencies, queryCompilationContext, subquery)
{
    protected override QueryableMethodTranslatingExpressionVisitor CreateSubqueryVisitor()
        => new MimironDb2QueryableMethodTranslatingExpressionVisitor(dependencies, queryCompilationContext, subquery: true);

    protected override ShapedQueryExpression CreateShapedQueryExpression(IEntityType entityType)
    {
        ArgumentNullException.ThrowIfNull(entityType);

        var queryExpression = new Expressions.Db2QueryExpression(entityType);

        var shaperExpression = new StructuralTypeShaperExpression(
            entityType,
            new ProjectionBindingExpression(queryExpression, new ProjectionMember(), typeof(ValueBuffer)),
            nullable: false);

        return new ShapedQueryExpression(queryExpression, shaperExpression);
    }

    protected override ShapedQueryExpression? TranslateAll(ShapedQueryExpression source, LambdaExpression predicate)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(predicate);

        // Bootstrap semantics: All(p) == !Any(!p)
        var negatedBody = Expression.Not(predicate.Body);
        var negated = Expression.Lambda(negatedBody, predicate.Parameters);

        var any = TranslateAny(source, negated)
            ?? throw new NotSupportedException("MimironDb2 failed to translate All() during bootstrap.");

        if (any.QueryExpression is not Expressions.Db2QueryExpression db2QueryExpression)
            throw new NotSupportedException("MimironDb2 query translation requires Db2QueryExpression as the query root.");

        db2QueryExpression.ApplyScalarNegation();

        return any;
    }

    protected override ShapedQueryExpression? TranslateAny(ShapedQueryExpression source, LambdaExpression? predicate)
    {
        ArgumentNullException.ThrowIfNull(source);

        // Bootstrap semantics: Any() -> Take(1).Select(_ => true).SingleOrDefault()
        if (predicate is not null)
        {
            source = TranslateWhere(source, predicate)
                ?? throw new NotSupportedException("MimironDb2 failed to translate Any() predicate during bootstrap.");
        }

        source = TranslateTake(source, Expression.Constant(1))
            ?? throw new NotSupportedException("MimironDb2 failed to translate Any() Take(1) during bootstrap.");

        var selectorParam = Expression.Parameter(source.ShaperExpression.Type, "_ignored");
        var selector = Expression.Lambda(Expression.Constant(true), selectorParam);
        source = TranslateSelect(source, selector)
            ?? throw new NotSupportedException("MimironDb2 failed to translate Any() projection during bootstrap.");

        return source.UpdateResultCardinality(ResultCardinality.SingleOrDefault);
    }

    protected override ShapedQueryExpression? TranslateAverage(ShapedQueryExpression source, LambdaExpression? selector, Type resultType)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateCast(ShapedQueryExpression source, Type resultType)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateConcat(ShapedQueryExpression source1, ShapedQueryExpression source2)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateContains(ShapedQueryExpression source, Expression item)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateCount(ShapedQueryExpression source, LambdaExpression? predicate)
    {
        ArgumentNullException.ThrowIfNull(source);

        if (source.QueryExpression is not Expressions.Db2QueryExpression queryExpression)
            throw new NotSupportedException("MimironDb2 query translation requires Db2QueryExpression as the query root.");

        // Bootstrap semantics: Count(p?) is executed client-side by enumerating the query result.
        if (predicate is not null)
        {
            source = TranslateWhere(source, predicate)
                ?? throw new NotSupportedException("MimironDb2 failed to translate Count() predicate during bootstrap.");
        }

        // Project a constant so the shaper result type becomes int.
        var selectorParam = Expression.Parameter(source.ShaperExpression.Type, "_ignored");
        var selector = Expression.Lambda(Expression.Constant(1), selectorParam);
        source = TranslateSelect(source, selector)
            ?? throw new NotSupportedException("MimironDb2 failed to translate Count() projection during bootstrap.");

        queryExpression.ApplyTerminalOperator(Expressions.Db2QueryExpression.Db2TerminalOperator.Count);

        // EF expects scalar result.
        return source.UpdateResultCardinality(ResultCardinality.Single);
    }

    protected override ShapedQueryExpression? TranslateDefaultIfEmpty(ShapedQueryExpression source, Expression? defaultValue)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateDistinct(ShapedQueryExpression source)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression TranslateElementAtOrDefault(ShapedQueryExpression source, Expression index, bool returnDefault)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateExcept(ShapedQueryExpression source1, ShapedQueryExpression source2)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateFirstOrDefault(ShapedQueryExpression source, LambdaExpression? predicate, Type returnType, bool returnDefault)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(returnType);

        // Correctness-first, client-side execution for now:
        // - Apply predicate via the existing Where pipeline.
        // - Apply Take(1) to preserve First/FirstOrDefault semantics.
        if (predicate is not null)
        {
            source = TranslateWhere(source, predicate)
                ?? throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");
        }

        source = TranslateTake(source, Expression.Constant(1))
            ?? throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

        // EF Core represents First/FirstOrDefault as single-result cardinality over a limited sequence.
        return source.UpdateResultCardinality(returnDefault ? ResultCardinality.SingleOrDefault : ResultCardinality.Single);
    }

    protected override ShapedQueryExpression? TranslateGroupBy(
        ShapedQueryExpression source,
        LambdaExpression keySelector,
        LambdaExpression? elementSelector,
        LambdaExpression? resultSelector)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateGroupJoin(
        ShapedQueryExpression outer,
        ShapedQueryExpression inner,
        LambdaExpression outerKeySelector,
        LambdaExpression innerKeySelector,
        LambdaExpression resultSelector)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateIntersect(ShapedQueryExpression source1, ShapedQueryExpression source2)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateJoin(
        ShapedQueryExpression outer,
        ShapedQueryExpression inner,
        LambdaExpression outerKeySelector,
        LambdaExpression innerKeySelector,
        LambdaExpression resultSelector)
        => TranslateJoinCore(
            joinOperator: nameof(Queryable.Join),
            outer,
            inner,
            outerKeySelector,
            innerKeySelector,
            resultSelector);

    protected override ShapedQueryExpression? TranslateLeftJoin(
        ShapedQueryExpression outer,
        ShapedQueryExpression inner,
        LambdaExpression outerKeySelector,
        LambdaExpression innerKeySelector,
        LambdaExpression resultSelector)
        => TranslateJoinCore(
            joinOperator: nameof(Queryable.LeftJoin),
            outer,
            inner,
            outerKeySelector,
            innerKeySelector,
            resultSelector);

    protected override ShapedQueryExpression? TranslateRightJoin(
        ShapedQueryExpression outer,
        ShapedQueryExpression inner,
        LambdaExpression outerKeySelector,
        LambdaExpression innerKeySelector,
        LambdaExpression resultSelector)
        => TranslateJoinCore(
            joinOperator: nameof(Queryable.RightJoin),
            outer,
            inner,
            outerKeySelector,
            innerKeySelector,
            resultSelector);

    protected override ShapedQueryExpression? TranslateLastOrDefault(ShapedQueryExpression source, LambdaExpression? predicate, Type returnType, bool returnDefault)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(returnType);

        // Correctness-first, client-side execution for now.
        if (predicate is not null)
        {
            source = TranslateWhere(source, predicate)
                ?? throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");
        }

        if (source.QueryExpression is not Expressions.Db2QueryExpression queryExpression)
            throw new NotSupportedException("MimironDb2 query translation requires Db2QueryExpression as the query root.");

        // If ordered, implement Last/LastOrDefault by reversing the orderings and taking the first element.
        if (queryExpression.Orderings.Count > 0)
        {
            var existing = queryExpression.Orderings.ToArray();
            queryExpression.Orderings.Clear();

            foreach (var (keySelector, ascending) in existing)
                queryExpression.ApplyOrdering(keySelector, !ascending);

            source = TranslateTake(source, Expression.Constant(1))
                ?? throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

            return source.UpdateResultCardinality(returnDefault ? ResultCardinality.SingleOrDefault : ResultCardinality.Single);
        }

        // If not ordered, use natural file order for Last/LastOrDefault.
        queryExpression.ApplyTerminalOperator(
            returnDefault
                ? Expressions.Db2QueryExpression.Db2TerminalOperator.LastOrDefault
                : Expressions.Db2QueryExpression.Db2TerminalOperator.Last);

        return source.UpdateResultCardinality(returnDefault ? ResultCardinality.SingleOrDefault : ResultCardinality.Single);
    }

    protected override ShapedQueryExpression? TranslateLongCount(ShapedQueryExpression source, LambdaExpression? predicate)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateMax(ShapedQueryExpression source, LambdaExpression? selector, Type resultType)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateMin(ShapedQueryExpression source, LambdaExpression? selector, Type resultType)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateOfType(ShapedQueryExpression source, Type resultType)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateOrderBy(ShapedQueryExpression source, LambdaExpression keySelector, bool ascending)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(keySelector);

        if (source.QueryExpression is not Expressions.Db2QueryExpression queryExpression)
            throw new NotSupportedException("MimironDb2 query translation requires Db2QueryExpression as the query root.");

        // EF Core semantics: OrderBy resets prior orderings.
        queryExpression.Orderings.Clear();
        queryExpression.ApplyOrdering(keySelector, ascending);
        return source;
    }

    protected override ShapedQueryExpression? TranslateReverse(ShapedQueryExpression source)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateSelect(ShapedQueryExpression source, LambdaExpression selector)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(selector);

        // Compose the selector over the existing shaper.
        var newShaper = Replace(selector.Body, selector.Parameters[0], source.ShaperExpression);
        return source.UpdateShaperExpression(newShaper);
    }

    protected override ShapedQueryExpression? TranslateSelectMany(ShapedQueryExpression source, LambdaExpression selector)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateSelectMany(ShapedQueryExpression source, LambdaExpression collectionSelector, LambdaExpression resultSelector)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateSingleOrDefault(ShapedQueryExpression source, LambdaExpression? predicate, Type returnType, bool returnDefault)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(returnType);

        // Correctness-first, client-side execution for now:
        // - Apply predicate via the existing Where pipeline.
        // - Apply Take(2) so Single/SingleOrDefault can detect multiple matches.
        if (predicate is not null)
        {
            source = TranslateWhere(source, predicate)
                ?? throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");
        }

        source = TranslateTake(source, Expression.Constant(2))
            ?? throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

        return source.UpdateResultCardinality(returnDefault ? ResultCardinality.SingleOrDefault : ResultCardinality.Single);
    }

    protected override ShapedQueryExpression? TranslateSkip(ShapedQueryExpression source, Expression count)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(count);

        if (source.QueryExpression is not Expressions.Db2QueryExpression queryExpression)
            throw new NotSupportedException("MimironDb2 query translation requires Db2QueryExpression as the query root.");

        if (count.Type != typeof(int))
            throw new NotSupportedException("MimironDb2 currently only supports Skip() with an int count expression.");

        queryExpression.ApplyOffset(count);
        return source;
    }

    protected override ShapedQueryExpression? TranslateSkipWhile(ShapedQueryExpression source, LambdaExpression predicate)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateSum(ShapedQueryExpression source, LambdaExpression? selector, Type resultType)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateTake(ShapedQueryExpression source, Expression count)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(count);

        if (source.QueryExpression is not Expressions.Db2QueryExpression queryExpression)
            throw new NotSupportedException("MimironDb2 query translation requires Db2QueryExpression as the query root.");

        // EF Core will frequently parameterize Take() counts; capture the expression and evaluate at execution time.
        if (count.Type != typeof(int))
            throw new NotSupportedException("MimironDb2 currently only supports Take() with an int count expression.");

        queryExpression.ApplyLimit(count);
        return source;
    }

    private sealed class ValueBufferIndexOffsetVisitor(int offset) : ExpressionVisitor
    {
        protected override Expression VisitIndex(IndexExpression node)
        {
            if (offset == 0)
                return base.VisitIndex(node);

            if (node.Object?.Type != typeof(ValueBuffer))
                return base.VisitIndex(node);

            var visitedObject = Visit(node.Object);
            var anyRewritten = false;
            var newArgs = new Expression[node.Arguments.Count];

            for (var argIndex = 0; argIndex < node.Arguments.Count; argIndex++)
            {
                var arg = node.Arguments[argIndex];
                if (arg is ConstantExpression { Type: { } t, Value: int i } && t == typeof(int))
                {
                    newArgs[argIndex] = Expression.Constant(i + offset);
                    anyRewritten = true;
                    continue;
                }

                newArgs[argIndex] = Visit(arg);
            }

            return anyRewritten
                ? Expression.MakeIndex(visitedObject, node.Indexer, newArgs)
                : base.VisitIndex(node);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (offset == 0)
                return base.VisitMethodCall(node);

            // EF Core shapers typically read from ValueBuffer via either:
            // - ValueBufferTryReadValue<T>(..., int index, ...)
            // - valueBuffer.get_Item(int index)
            // When joining, inner-side reads must be offset so the combined ValueBuffer can store
            // outer values at [0..outerLen) and inner values at [outerLen..outerLen+innerLen).

            var hasValueBuffer = node.Object?.Type == typeof(ValueBuffer)
                || node.Arguments.Any(static a => a.Type == typeof(ValueBuffer) || a.Type == typeof(ValueBuffer).MakeByRefType());

            if (!hasValueBuffer)
                return base.VisitMethodCall(node);

            var visitedObject = Visit(node.Object);
            var anyRewritten = false;
            var newArgs = new Expression[node.Arguments.Count];

            for (var argIndex = 0; argIndex < node.Arguments.Count; argIndex++)
            {
                var arg = node.Arguments[argIndex];
                if (arg is ConstantExpression { Type: { } t, Value: int i } && t == typeof(int))
                {
                    newArgs[argIndex] = Expression.Constant(i + offset);
                    anyRewritten = true;
                    continue;
                }

                newArgs[argIndex] = Visit(arg);
            }

            if (anyRewritten)
                return Expression.Call(visitedObject, node.Method, newArgs);

            return base.VisitMethodCall(node);
        }
    }

    private static ShapedQueryExpression TranslateJoinCore(
        string joinOperator,
        ShapedQueryExpression outer,
        ShapedQueryExpression inner,
        LambdaExpression outerKeySelector,
        LambdaExpression innerKeySelector,
        LambdaExpression resultSelector)
    {
        ArgumentNullException.ThrowIfNull(outer);
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(outerKeySelector);
        ArgumentNullException.ThrowIfNull(innerKeySelector);
        ArgumentNullException.ThrowIfNull(resultSelector);

        if (outer.QueryExpression is not Expressions.Db2QueryExpression outerQuery
            || inner.QueryExpression is not Expressions.Db2QueryExpression innerQuery)
        {
            throw new NotSupportedException("MimironDb2 joins currently require Db2QueryExpression on both sides.");
        }

        outerQuery.ApplyJoin(joinOperator, innerQuery, outerKeySelector, innerKeySelector);

        // Join shapers for outer and inner both read from a single ValueBuffer; offset the inner shaper's
        // ValueBuffer reads to avoid index collisions.
        //
        // IMPORTANT: The offset must match the ValueBuffer layout produced by the bootstrap executor
        // (Table(...)), which currently projects a fixed span per entity based on the entity's property
        // indexes (not the subset of columns referenced by the shaper).
        var outerValueBufferLength = GetJoinedOuterValueBufferLength(outerQuery);

        var innerShaperExpression = joinOperator == nameof(Queryable.LeftJoin)
            ? MakeStructuralTypeShapersNullable(inner.ShaperExpression)
            : inner.ShaperExpression;

        var innerShaper = outerValueBufferLength == 0
            ? innerShaperExpression
            : new ValueBufferIndexOffsetVisitor(outerValueBufferLength).Visit(innerShaperExpression);

        var shaper = resultSelector.Body;
        shaper = Replace(shaper, resultSelector.Parameters[0], outer.ShaperExpression);
        shaper = Replace(shaper, resultSelector.Parameters[1], innerShaper);

        return new ShapedQueryExpression(outerQuery, shaper);
    }

    private static int GetJoinedOuterValueBufferLength(Expressions.Db2QueryExpression outerQuery)
    {
        ArgumentNullException.ThrowIfNull(outerQuery);

        var length = GetEntityValueBufferLength(outerQuery.EntityType);

        // All joins in the query expression represent a left-deep join tree; the outer shape at this point
        // includes the root entity plus all inner entities from previous joins.
        // The join currently being translated is the last one that was appended.
        var joins = outerQuery.Joins;
        for (var i = 0; i < joins.Count - 1; i++)
        {
            length += GetEntityValueBufferLength(joins[i].Inner.EntityType);
        }

        return length;
    }

    private static int GetEntityValueBufferLength(IEntityType entityType)
    {
        ArgumentNullException.ThrowIfNull(entityType);

        if (!entityType.GetProperties().Any())
            return 0;

        var maxIndex = entityType.GetProperties().Max(static p => p.GetIndex());
        return maxIndex < 0 ? 0 : maxIndex + 1;
    }

    private static Expression MakeStructuralTypeShapersNullable(Expression expression)
        => new StructuralTypeShaperNullableVisitor().Visit(expression);

    private sealed class StructuralTypeShaperNullableVisitor : ExpressionVisitor
    {
        protected override Expression VisitExtension(Expression node)
        {
            if (node is StructuralTypeShaperExpression shaper)
            {
                // EF Core uses nullable shapers to represent the inner side of left joins.
                return shaper.MakeNullable();
            }

            return base.VisitExtension(node);
        }
    }

    private static Expression Replace(Expression body, ParameterExpression parameter, Expression replacement)
        => new ParameterReplaceVisitor(parameter, replacement).Visit(body);

    private sealed class ParameterReplaceVisitor(ParameterExpression parameter, Expression replacement) : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node)
            => node == parameter ? replacement : base.VisitParameter(node);
    }

    protected override ShapedQueryExpression? TranslateTakeWhile(ShapedQueryExpression source, LambdaExpression predicate)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateThenBy(ShapedQueryExpression source, LambdaExpression keySelector, bool ascending)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(keySelector);

        if (source.QueryExpression is not Expressions.Db2QueryExpression queryExpression)
            throw new NotSupportedException("MimironDb2 query translation requires Db2QueryExpression as the query root.");

        queryExpression.ApplyOrdering(keySelector, ascending);
        return source;
    }

    protected override ShapedQueryExpression? TranslateUnion(ShapedQueryExpression source1, ShapedQueryExpression source2)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateWhere(ShapedQueryExpression source, LambdaExpression predicate)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(predicate);

        if (source.QueryExpression is not Expressions.Db2QueryExpression queryExpression)
            throw new NotSupportedException("MimironDb2 query translation requires Db2QueryExpression as the query root.");

        queryExpression.ApplyPredicate(predicate);
        return source;
    }
}
