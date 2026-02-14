using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

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
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateAny(ShapedQueryExpression source, LambdaExpression? predicate)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateAverage(ShapedQueryExpression source, LambdaExpression? selector, Type resultType)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateCast(ShapedQueryExpression source, Type resultType)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateConcat(ShapedQueryExpression source1, ShapedQueryExpression source2)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateContains(ShapedQueryExpression source, Expression item)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateCount(ShapedQueryExpression source, LambdaExpression? predicate)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateDefaultIfEmpty(ShapedQueryExpression source, Expression? defaultValue)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateDistinct(ShapedQueryExpression source)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression TranslateElementAtOrDefault(ShapedQueryExpression source, Expression index, bool returnDefault)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateExcept(ShapedQueryExpression source1, ShapedQueryExpression source2)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateFirstOrDefault(ShapedQueryExpression source, LambdaExpression? predicate, Type returnType, bool returnDefault)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

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
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateLongCount(ShapedQueryExpression source, LambdaExpression? predicate)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateMax(ShapedQueryExpression source, LambdaExpression? selector, Type resultType)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateMin(ShapedQueryExpression source, LambdaExpression? selector, Type resultType)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateOfType(ShapedQueryExpression source, Type resultType)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateOrderBy(ShapedQueryExpression source, LambdaExpression keySelector, bool ascending)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

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
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

    protected override ShapedQueryExpression? TranslateSkip(ShapedQueryExpression source, Expression count)
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

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

        var shaper = resultSelector.Body;
        shaper = Replace(shaper, resultSelector.Parameters[0], outer.ShaperExpression);
        shaper = Replace(shaper, resultSelector.Parameters[1], inner.ShaperExpression);

        return new ShapedQueryExpression(outerQuery, shaper);
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
        => throw new NotSupportedException("MimironDb2 query translation is not implemented yet.");

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
