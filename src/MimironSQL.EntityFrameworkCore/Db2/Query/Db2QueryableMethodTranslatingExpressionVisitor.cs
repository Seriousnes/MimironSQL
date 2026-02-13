using System.Linq;
using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

using MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;
using MimironSQL.EntityFrameworkCore.Db2.Schema;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

/// <summary>
/// Translates LINQ queryable method calls into <see cref="Db2QueryExpression"/> operations.
/// Each <c>TranslateXxx</c> method takes a <see cref="ShapedQueryExpression"/> and mutates its
/// <see cref="Db2QueryExpression"/> to represent the new operation.
/// </summary>
internal sealed class Db2QueryableMethodTranslatingExpressionVisitor(
    QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
    QueryCompilationContext queryCompilationContext)
    : QueryableMethodTranslatingExpressionVisitor(dependencies, queryCompilationContext, subquery: false)
{
    private readonly Db2ExpressionTranslator _expressionTranslator = new();

    /// <summary>
    /// Private constructor for subquery visitors.
    /// </summary>
    private Db2QueryableMethodTranslatingExpressionVisitor(
        Db2QueryableMethodTranslatingExpressionVisitor parentVisitor)
        : this(parentVisitor.Dependencies, parentVisitor.QueryCompilationContext)
    {
    }

    protected override ShapedQueryExpression CreateShapedQueryExpression(IEntityType entityType)
    {
        var queryExpression = new Db2QueryExpression(entityType);

        return new ShapedQueryExpression(
            queryExpression,
            new StructuralTypeShaperExpression(
                entityType,
                new ProjectionBindingExpression(queryExpression, new ProjectionMember(), typeof(ValueBuffer)),
                nullable: false));
    }

    protected override ShapedQueryExpression? TranslateWhere(ShapedQueryExpression source, LambdaExpression predicate)
    {
        // Handle joined query expressions
        if (source.QueryExpression is Db2JoinedQueryExpression joinedQuery)
        {
            return TranslateJoinedWhere(source, joinedQuery, predicate);
        }

        var db2Query = (Db2QueryExpression)source.QueryExpression;
        var entityType = db2Query.EntityType;

        var filter = _expressionTranslator.Translate(predicate, entityType);
        if (filter is null)
        {
            // Cannot translate — signal to EF Core to evaluate client-side or throw
            return null;
        }

        db2Query.ApplyFilter(filter);
        return source;
    }

    /// <summary>
    /// Translates a Where clause on a joined query.
    /// The predicate may access properties from both the outer and inner entities.
    /// </summary>
    private ShapedQueryExpression? TranslateJoinedWhere(
        ShapedQueryExpression source,
        Db2JoinedQueryExpression joinedQuery,
        LambdaExpression predicate)
    {
        // Translate the joined predicate using a specialized translator
        var filter = _expressionTranslator.TranslateJoined(
            predicate,
            joinedQuery.Outer.EntityType,
            joinedQuery.InnerEntityType,
            joinedQuery.InnerTableName,
            source.ShaperExpression);

        if (filter is null)
        {
            // Cannot translate — signal to EF Core to evaluate client-side or throw
            return null;
        }

        joinedQuery.ApplyFilter(filter);
        return source;
    }

    protected override ShapedQueryExpression? TranslateFirstOrDefault(
        ShapedQueryExpression source,
        LambdaExpression? predicate,
        Type returnType,
        bool returnDefault)
    {
        if (predicate is not null)
        {
            var filtered = TranslateWhere(source, predicate);
            if (filtered is null) return null;
            source = filtered;
        }

        if (source.QueryExpression is Db2JoinedQueryExpression joinedQuery)
        {
            joinedQuery.ApplyLimit(1);
        }
        else
        {
            var db2Query = (Db2QueryExpression)source.QueryExpression;
            db2Query.ApplyLimit(1);
        }

        return source.UpdateResultCardinality(ResultCardinality.SingleOrDefault);
    }

    protected override ShapedQueryExpression? TranslateSingleOrDefault(
        ShapedQueryExpression source,
        LambdaExpression? predicate,
        Type returnType,
        bool returnDefault)
    {
        if (predicate is not null)
        {
            var filtered = TranslateWhere(source, predicate);
            if (filtered is null) return null;
            source = filtered;
        }

        if (source.QueryExpression is Db2JoinedQueryExpression joinedQuery)
        {
            joinedQuery.ApplyLimit(2); // Take 2 to detect multiple results
        }
        else
        {
            var db2Query = (Db2QueryExpression)source.QueryExpression;
            db2Query.ApplyLimit(2); // Take 2 to detect multiple results
        }

        return source.UpdateResultCardinality(ResultCardinality.SingleOrDefault);
    }

    protected override ShapedQueryExpression? TranslateCount(ShapedQueryExpression source, LambdaExpression? predicate)
    {
        if (predicate is not null)
        {
            var filtered = TranslateWhere(source, predicate);
            if (filtered is null) return null;
            source = filtered;
        }

        // Terminal scalar operator
        if (source.QueryExpression is not (Db2QueryExpression or Db2JoinedQueryExpression))
            return null;

        return source
            .UpdateShaperExpression(new Db2ScalarAggregateExpression(Db2ScalarAggregateKind.Count, negate: false))
            .UpdateResultCardinality(ResultCardinality.Single);
    }

    protected override ShapedQueryExpression? TranslateAny(ShapedQueryExpression source, LambdaExpression? predicate)
    {
        if (predicate is not null)
        {
            var filtered = TranslateWhere(source, predicate);
            if (filtered is null) return null;
            source = filtered;
        }

        // Terminal scalar operator
        if (source.QueryExpression is not (Db2QueryExpression or Db2JoinedQueryExpression))
            return null;

        return source
            .UpdateShaperExpression(new Db2ScalarAggregateExpression(Db2ScalarAggregateKind.Any, negate: false))
            .UpdateResultCardinality(ResultCardinality.Single);
    }

    protected override ShapedQueryExpression? TranslateAll(ShapedQueryExpression source, LambdaExpression predicate)
    {
        // ALL(source, p) == NOT ANY(source, x => NOT p(x))
        var negatedBody = Expression.Not(predicate.Body);
        var negatedPredicate = Expression.Lambda(negatedBody, predicate.Parameters);

        var anyViolations = TranslateAny(source, negatedPredicate);
        if (anyViolations is null)
            return null;

        return anyViolations
            .UpdateShaperExpression(new Db2ScalarAggregateExpression(Db2ScalarAggregateKind.Any, negate: true))
            .UpdateResultCardinality(ResultCardinality.Single);
    }

    protected override ShapedQueryExpression? TranslateTake(ShapedQueryExpression source, Expression count)
    {
        // Handle joined queries
        if (source.QueryExpression is Db2JoinedQueryExpression joinedQuery)
        {
            if (count is ConstantExpression { Value: int intCount })
            {
                joinedQuery.ApplyLimit(intCount);
                return source;
            }

            if (count is ParameterExpression paramExpr)
            {
                joinedQuery.ApplyLimitParameter(paramExpr.Name!);
                return source;
            }

            if (count is QueryParameterExpression queryParamExpr)
            {
                joinedQuery.ApplyLimitParameter(queryParamExpr.Name);
                return source;
            }

            return null;
        }

        var db2Query = (Db2QueryExpression)source.QueryExpression;

        if (count is ConstantExpression { Value: int intCountVal })
        {
            db2Query.ApplyLimit(intCountVal);
            return source;
        }

        if (count is ParameterExpression paramExpr2)
        {
            db2Query.ApplyLimitParameter(paramExpr2.Name!);
            return source;
        }

        if (count is QueryParameterExpression queryParamExpr2)
        {
            db2Query.ApplyLimitParameter(queryParamExpr2.Name);
            return source;
        }

        return null;
    }

    protected override ShapedQueryExpression? TranslateSkip(ShapedQueryExpression source, Expression count)
    {
        // Handle joined queries
        if (source.QueryExpression is Db2JoinedQueryExpression joinedQuery)
        {
            if (count is ConstantExpression { Value: int intCount })
            {
                joinedQuery.ApplyOffset(intCount);
                return source;
            }

            if (count is ParameterExpression paramExpr)
            {
                joinedQuery.ApplyOffsetParameter(paramExpr.Name!);
                return source;
            }

            if (count is QueryParameterExpression queryParamExpr)
            {
                joinedQuery.ApplyOffsetParameter(queryParamExpr.Name);
                return source;
            }

            return null;
        }

        var db2Query = (Db2QueryExpression)source.QueryExpression;

        if (count is ConstantExpression { Value: int intCountVal })
        {
            db2Query.ApplyOffset(intCountVal);
            return source;
        }

        if (count is ParameterExpression paramExpr2)
        {
            db2Query.ApplyOffsetParameter(paramExpr2.Name!);
            return source;
        }

        if (count is QueryParameterExpression queryParamExpr2)
        {
            db2Query.ApplyOffsetParameter(queryParamExpr2.Name);
            return source;
        }

        return null;
    }

    protected override ShapedQueryExpression? TranslateOrderBy(ShapedQueryExpression source, LambdaExpression keySelector, bool ascending)
    {
        var db2Query = (Db2QueryExpression)source.QueryExpression;
        db2Query.ClearOrderings(); // OrderBy replaces all previous orderings

        var fieldAccess = TryResolveFieldFromSelector(keySelector, db2Query.EntityType);
        if (fieldAccess is null) return null;

        db2Query.ApplyOrdering(new Db2OrderingExpression(fieldAccess, ascending));
        return source;
    }

    protected override ShapedQueryExpression? TranslateThenBy(ShapedQueryExpression source, LambdaExpression keySelector, bool ascending)
    {
        var db2Query = (Db2QueryExpression)source.QueryExpression;

        var fieldAccess = TryResolveFieldFromSelector(keySelector, db2Query.EntityType);
        if (fieldAccess is null) return null;

        db2Query.ApplyOrdering(new Db2OrderingExpression(fieldAccess, ascending));
        return source;
    }

    protected override ShapedQueryExpression? TranslateDistinct(ShapedQueryExpression source)
        => null; // Client-evaluated for now

    protected override ShapedQueryExpression TranslateSelect(ShapedQueryExpression source, LambdaExpression selector)
    {
        // Identity projection — no-op
        if (selector.Body == selector.Parameters[0])
            return source;

        // Disallow projecting an entire navigation entity (e.g. x => new { x.Nav, NavId = x.Nav.Id }).
        // This provider only supports navigation access for filtering/includes, not projecting navigation entities.
        if (source.QueryExpression is Db2QueryExpression db2Query
            && ContainsNavigationEntityProjection(selector.Body, db2Query.EntityType, selector.Parameters[0]))
        {
            throw new NotSupportedException("Projecting navigation entities is not supported by the Db2 provider.");
        }

        if (source.QueryExpression is Db2JoinedQueryExpression joinedQueryForProjectionCheck
            && ContainsNavigationEntityProjection(selector.Body, joinedQueryForProjectionCheck.Outer.EntityType, selector.Parameters[0]))
        {
            throw new NotSupportedException("Projecting navigation entities is not supported by the Db2 provider.");
        }

        // Remap the lambda body: replace the lambda parameter with the current shaper expression (typically
        // a StructuralTypeShaperExpression) so that member accesses like x.Id become <ShaperExpression>.Id.
        var newSelectorBody = RemapLambdaBody(source, selector);

        var rootEntityType = source.QueryExpression switch
        {
            Db2QueryExpression q => q.EntityType,
            Db2JoinedQueryExpression jq => jq.Outer.EntityType,
            _ => null,
        };

        // As a last line of defense, disallow projecting *any* entity-typed value other than the root entity.
        // This catches navigation entity projections even when EF rewrites navigation access into EF internals.
        if (rootEntityType is not null
            && (ContainsNonRootEntityTypedValue(selector.Body, rootEntityType)
                || ContainsNonRootEntityTypedValue(newSelectorBody, rootEntityType)))
        {
            throw new NotSupportedException("Projecting navigation entities is not supported by the Db2 provider.");
        }

        // After navigation expansion, selecting a navigation entity often shows up as a non-root
        // StructuralTypeShaperExpression embedded inside the projection.
        if (rootEntityType is not null
            && ContainsNonRootEntityProjection(newSelectorBody, rootEntityType))
        {
            throw new NotSupportedException("Projecting navigation entities is not supported by the Db2 provider.");
        }

        // If EF expanded navigation projection into a join, the navigation entity shows up as TransparentIdentifier.Inner.
        if (source.QueryExpression is Db2JoinedQueryExpression
            && ContainsJoinedInnerEntityProjection(newSelectorBody))
        {
            throw new NotSupportedException("Projecting navigation entities is not supported by the Db2 provider.");
        }

        return source.UpdateShaperExpression(newSelectorBody);
    }

    private bool ContainsNonRootEntityTypedValue(Expression expression, IEntityType rootEntityType)
        => new NonRootEntityTypedValueDetector(QueryCompilationContext.Model, rootEntityType)
            .Contains(expression);

    private sealed class NonRootEntityTypedValueDetector(IModel model, IEntityType rootEntityType)
    {
        private readonly IModel _model = model;
        private readonly IEntityType _rootEntityType = rootEntityType;

        public bool Contains(Expression expression)
            => Visit(expression, inMemberChain: false);

        private bool Visit(Expression? expression, bool inMemberChain)
        {
            if (expression is null)
                return false;

            if (!inMemberChain
                && _model.FindEntityType(expression.Type) is IEntityType entityType
                && !ReferenceEquals(entityType, _rootEntityType))
            {
                return true;
            }

            return expression switch
            {
                MemberExpression m => Visit(m.Expression, inMemberChain: true),
                MethodCallExpression mc => Visit(mc.Object, inMemberChain: false)
                    || mc.Arguments.Any(a => Visit(a, inMemberChain: false)),
                NewExpression ne => ne.Arguments.Any(a => Visit(a, inMemberChain: false)),
                MemberInitExpression mi => Visit(mi.NewExpression, inMemberChain: false)
                    || mi.Bindings.OfType<MemberAssignment>().Any(b => Visit(b.Expression, inMemberChain: false)),
                UnaryExpression ue => Visit(ue.Operand, inMemberChain: false),
                BinaryExpression be => Visit(be.Left, inMemberChain: false) || Visit(be.Right, inMemberChain: false),
                ConditionalExpression ce => Visit(ce.Test, inMemberChain: false)
                    || Visit(ce.IfTrue, inMemberChain: false)
                    || Visit(ce.IfFalse, inMemberChain: false),
                _ => false,
            };
        }
    }

    private static bool ContainsNonRootEntityProjection(Expression expression, IEntityType rootEntityType)
        => new NonRootEntityProjectionDetector(rootEntityType).Contains(expression);

    private sealed class NonRootEntityProjectionDetector(IEntityType rootEntityType)
    {
        private readonly IEntityType _rootEntityType = rootEntityType;

        public bool Contains(Expression expression)
            => Visit(expression, inMemberChain: false);

        private bool Visit(Expression? expression, bool inMemberChain)
        {
            if (expression is null)
                return false;

            if (!inMemberChain
                && expression is StructuralTypeShaperExpression { StructuralType: IEntityType entityType }
                && !ReferenceEquals(entityType, _rootEntityType))
            {
                return true;
            }

            return expression switch
            {
                MemberExpression m => Visit(m.Expression, inMemberChain: true),
                MethodCallExpression mc => Visit(mc.Object, inMemberChain: false)
                    || mc.Arguments.Any(a => Visit(a, inMemberChain: false)),
                NewExpression ne => ne.Arguments.Any(a => Visit(a, inMemberChain: false)),
                MemberInitExpression mi => Visit(mi.NewExpression, inMemberChain: false)
                    || mi.Bindings.OfType<MemberAssignment>().Any(b => Visit(b.Expression, inMemberChain: false)),
                UnaryExpression ue => Visit(ue.Operand, inMemberChain: false),
                BinaryExpression be => Visit(be.Left, inMemberChain: false) || Visit(be.Right, inMemberChain: false),
                ConditionalExpression ce => Visit(ce.Test, inMemberChain: false)
                    || Visit(ce.IfTrue, inMemberChain: false)
                    || Visit(ce.IfFalse, inMemberChain: false),
                _ => false,
            };
        }
    }

    private static bool ContainsNavigationEntityProjection(Expression expression, IEntityType entityType, ParameterExpression rootParameter)
        => new NavigationEntityProjectionDetector(entityType, rootParameter).ContainsNavigationEntityProjection(expression);

    private sealed class NavigationEntityProjectionDetector(IEntityType entityType, ParameterExpression rootParameter)
    {
        private readonly IEntityType _entityType = entityType;
        private readonly ParameterExpression _rootParameter = rootParameter;

        public bool ContainsNavigationEntityProjection(Expression expression)
            => Visit(expression, inMemberChain: false);

        private bool Visit(Expression? expression, bool inMemberChain)
        {
            if (expression is null)
                return false;

            // Only flag a navigation member access when it is used as a value (not as the receiver for further member access).
            if (expression is MemberExpression member
                && member.Expression == _rootParameter
                && member.Member is System.Reflection.PropertyInfo property
                && _entityType.FindNavigation(property.Name) is not null
                && !inMemberChain)
            {
                return true;
            }

            // EF can rewrite navigation access into EF.Property<T>(x, "Nav")
            if (expression is MethodCallExpression
                {
                    Method: { Name: nameof(EF.Property), DeclaringType: not null } method,
                    Arguments.Count: 2,
                    Object: null,
                } efPropertyCall
                && method.DeclaringType == typeof(EF)
                && efPropertyCall.Arguments[0] == _rootParameter
                && efPropertyCall.Arguments[1] is ConstantExpression { Value: string navigationName }
                && _entityType.FindNavigation(navigationName) is not null
                && !inMemberChain)
            {
                return true;
            }

            return expression switch
            {
                MemberExpression m => Visit(m.Expression, inMemberChain: true),
                MethodCallExpression mc => Visit(mc.Object, inMemberChain: false)
                    || mc.Arguments.Any(a => Visit(a, inMemberChain: false)),
                NewExpression ne => ne.Arguments.Any(a => Visit(a, inMemberChain: false)),
                MemberInitExpression mi => Visit(mi.NewExpression, inMemberChain: false)
                    || mi.Bindings.OfType<MemberAssignment>().Any(b => Visit(b.Expression, inMemberChain: false)),
                UnaryExpression ue => Visit(ue.Operand, inMemberChain: false),
                BinaryExpression be => Visit(be.Left, inMemberChain: false) || Visit(be.Right, inMemberChain: false),
                ConditionalExpression ce => Visit(ce.Test, inMemberChain: false)
                    || Visit(ce.IfTrue, inMemberChain: false)
                    || Visit(ce.IfFalse, inMemberChain: false),
                _ => false,
            };
        }
    }

    private static bool ContainsJoinedInnerEntityProjection(Expression expression)
        => new JoinedInnerEntityProjectionDetector().Contains(expression);

    private sealed class JoinedInnerEntityProjectionDetector
    {
        public bool Contains(Expression expression)
            => Visit(expression, inMemberChain: false);

        private bool Visit(Expression? expression, bool inMemberChain)
        {
            if (expression is null)
                return false;

            if (expression is MemberExpression member
                && member.Member.Name == "Inner"
                && member.Expression is ParameterExpression
                && !inMemberChain)
            {
                return true;
            }

            return expression switch
            {
                MemberExpression m => Visit(m.Expression, inMemberChain: true),
                MethodCallExpression mc => Visit(mc.Object, inMemberChain: false)
                    || mc.Arguments.Any(a => Visit(a, inMemberChain: false)),
                NewExpression ne => ne.Arguments.Any(a => Visit(a, inMemberChain: false)),
                MemberInitExpression mi => Visit(mi.NewExpression, inMemberChain: false)
                    || mi.Bindings.OfType<MemberAssignment>().Any(b => Visit(b.Expression, inMemberChain: false)),
                UnaryExpression ue => Visit(ue.Operand, inMemberChain: false),
                BinaryExpression be => Visit(be.Left, inMemberChain: false) || Visit(be.Right, inMemberChain: false),
                ConditionalExpression ce => Visit(ce.Test, inMemberChain: false)
                    || Visit(ce.IfTrue, inMemberChain: false)
                    || Visit(ce.IfFalse, inMemberChain: false),
                _ => false,
            };
        }
    }

    protected override QueryableMethodTranslatingExpressionVisitor CreateSubqueryVisitor()
        => new Db2QueryableMethodTranslatingExpressionVisitor(this);

    // ── Unsupported operations (return null → EF Core throws "could not be translated") ──

    protected override ShapedQueryExpression? TranslateAverage(ShapedQueryExpression source, LambdaExpression? selector, Type resultType) => null;
    protected override ShapedQueryExpression? TranslateCast(ShapedQueryExpression source, Type castType) => null;
    protected override ShapedQueryExpression? TranslateConcat(ShapedQueryExpression source1, ShapedQueryExpression source2) => null;
    protected override ShapedQueryExpression? TranslateContains(ShapedQueryExpression source, Expression item) => null;
    protected override ShapedQueryExpression? TranslateDefaultIfEmpty(ShapedQueryExpression source, Expression? defaultValue) => null;
    protected override ShapedQueryExpression? TranslateElementAtOrDefault(ShapedQueryExpression source, Expression index, bool returnDefault) => null;
    protected override ShapedQueryExpression? TranslateExcept(ShapedQueryExpression source1, ShapedQueryExpression source2) => null;
    protected override ShapedQueryExpression? TranslateGroupBy(ShapedQueryExpression source, LambdaExpression keySelector, LambdaExpression? elementSelector, LambdaExpression? resultSelector) => null;
    protected override ShapedQueryExpression? TranslateGroupJoin(ShapedQueryExpression outer, ShapedQueryExpression inner, LambdaExpression outerKeySelector, LambdaExpression innerKeySelector, LambdaExpression resultSelector)
        => null; // Not supported - requires client evaluation
    protected override ShapedQueryExpression? TranslateIntersect(ShapedQueryExpression source1, ShapedQueryExpression source2) => null;
    protected override ShapedQueryExpression? TranslateJoin(ShapedQueryExpression outer, ShapedQueryExpression inner, LambdaExpression outerKeySelector, LambdaExpression innerKeySelector, LambdaExpression resultSelector)
        => TranslateJoinCore(outer, inner, outerKeySelector, innerKeySelector, resultSelector, isLeftJoin: false);
    protected override ShapedQueryExpression? TranslateLastOrDefault(ShapedQueryExpression source, LambdaExpression? predicate, Type returnType, bool returnDefault) => null;
    protected override ShapedQueryExpression? TranslateLongCount(ShapedQueryExpression source, LambdaExpression? predicate) => null;
    protected override ShapedQueryExpression? TranslateMax(ShapedQueryExpression source, LambdaExpression? selector, Type resultType) => null;
    protected override ShapedQueryExpression? TranslateMin(ShapedQueryExpression source, LambdaExpression? selector, Type resultType) => null;
    protected override ShapedQueryExpression? TranslateOfType(ShapedQueryExpression source, Type resultType) => null;
    protected override ShapedQueryExpression? TranslateReverse(ShapedQueryExpression source) => null;
    protected override ShapedQueryExpression? TranslateSelectMany(ShapedQueryExpression source, LambdaExpression collectionSelector, LambdaExpression resultSelector) => null;
    protected override ShapedQueryExpression? TranslateSkipWhile(ShapedQueryExpression source, LambdaExpression predicate) => null;
    protected override ShapedQueryExpression? TranslateSum(ShapedQueryExpression source, LambdaExpression? selector, Type resultType) => null;
    protected override ShapedQueryExpression? TranslateTakeWhile(ShapedQueryExpression source, LambdaExpression predicate) => null;
    protected override ShapedQueryExpression? TranslateLeftJoin(ShapedQueryExpression outer, ShapedQueryExpression inner, LambdaExpression outerKeySelector, LambdaExpression innerKeySelector, LambdaExpression resultSelector)
        => TranslateJoinCore(outer, inner, outerKeySelector, innerKeySelector, resultSelector, isLeftJoin: true);
    protected override ShapedQueryExpression? TranslateRightJoin(ShapedQueryExpression outer, ShapedQueryExpression inner, LambdaExpression outerKeySelector, LambdaExpression innerKeySelector, LambdaExpression resultSelector)
        => null; // Right joins not supported
    protected override ShapedQueryExpression? TranslateSelectMany(ShapedQueryExpression source, LambdaExpression selector) => null;
    protected override ShapedQueryExpression? TranslateUnion(ShapedQueryExpression source1, ShapedQueryExpression source2) => null;

    /// <summary>
    /// Core implementation for Join and LeftJoin translation.
    /// Creates a <see cref="Db2JoinedQueryExpression"/> that represents a join between two tables.
    /// </summary>
    private ShapedQueryExpression? TranslateJoinCore(
        ShapedQueryExpression outer,
        ShapedQueryExpression inner,
        LambdaExpression outerKeySelector,
        LambdaExpression innerKeySelector,
        LambdaExpression resultSelector,
        bool isLeftJoin)
    {
        // Only support joins where both sides are Db2QueryExpressions
        if (outer.QueryExpression is not Db2QueryExpression outerDb2Query
            || inner.QueryExpression is not Db2QueryExpression innerDb2Query)
        {
            return null;
        }

        // Extract the key columns from the selectors
        // Pattern: EF.Property<T>(entity, "PropertyName") or direct member access
        var outerKeyColumn = TryExtractKeyColumn(outerKeySelector, outerDb2Query.EntityType);
        var innerKeyColumn = TryExtractKeyColumn(innerKeySelector, innerDb2Query.EntityType);

        if (outerKeyColumn is null || innerKeyColumn is null)
        {
            return null;
        }

        // Create the joined query expression
        var joinedQuery = new Db2JoinedQueryExpression(
            outerDb2Query,
            innerDb2Query.EntityType,
            innerDb2Query.TableName,
            outerKeyColumn,
            innerKeyColumn,
            isLeftJoin);

        // Create a TransparentIdentifier-like shaper that combines outer and inner
        // The resultSelector's body tells us how to construct the result
        // Typically: (outer, inner) => new TransparentIdentifier<TOuter, TInner> { Outer = outer, Inner = inner }
        var outerShaper = outer.ShaperExpression;
        var innerShaper = inner.ShaperExpression;

        // Build a new shaper that represents the joined result
        // We use Db2JoinedShaperExpression to track both outer and inner shapers
        var joinedShaper = new Db2JoinedShaperExpression(
            outerShaper,
            innerShaper,
            resultSelector);

        return new ShapedQueryExpression(joinedQuery, joinedShaper);
    }

    /// <summary>
    /// Extracts the key column name from a join key selector.
    /// Handles both EF.Property calls and direct member access.
    /// </summary>
    private static string? TryExtractKeyColumn(LambdaExpression keySelector, IEntityType entityType)
    {
        var body = keySelector.Body;

        // Unwrap type conversions
        while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
            body = unary.Operand;

        // Handle EF.Property<T>(entity, "PropertyName")
        if (body is MethodCallExpression { Method: { Name: "Property", DeclaringType: { } dt } } propCall
            && dt == typeof(EF)
            && propCall.Arguments.Count == 2
            && propCall.Arguments[1] is ConstantExpression { Value: string propertyName })
        {
            var efProp = entityType.FindProperty(propertyName);
            return efProp?.GetColumnName() ?? propertyName;
        }

        // Handle direct member access: x.PropertyName
        if (body is MemberExpression { Member: System.Reflection.PropertyInfo memberProp } memberAccess
            && memberAccess.Expression == keySelector.Parameters[0])
        {
            var efProperty = entityType.FindProperty(memberProp.Name);
            return efProperty?.GetColumnName() ?? memberProp.Name;
        }

        return null;
    }

    // ── Helpers ──

    private static Db2FieldAccessExpression? TryResolveFieldFromSelector(LambdaExpression selector, IEntityType entityType)
    {
        // Handle x => x.Property (or <ShaperExpression>.Property after RemapLambdaBody)
        if (selector.Body is MemberExpression { Member: System.Reflection.PropertyInfo property } member
            && (member.Expression is ParameterExpression || member.Expression is StructuralTypeShaperExpression))
        {
            var efProperty = entityType.FindProperty(property.Name);
            if (efProperty is null) return null;

            var columnName = efProperty.GetColumnName() ?? efProperty.Name;
            var field = new Db2FieldSchema(
                columnName,
                default,
                ColumnStartIndex: -1,
                ElementCount: 1,
                IsVerified: false,
                IsVirtual: false,
                IsId: efProperty.IsPrimaryKey(),
                IsRelation: false,
                ReferencedTableName: null);

            return new Db2FieldAccessExpression(field, fieldIndex: -1, property.PropertyType);
        }

        return null;
    }

    private static Expression RemapLambdaBody(ShapedQueryExpression shapedQueryExpression, LambdaExpression lambdaExpression)
        => ReplacingExpressionVisitor.Replace(
            lambdaExpression.Parameters.Single(), shapedQueryExpression.ShaperExpression, lambdaExpression.Body);
}
