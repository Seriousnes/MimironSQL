using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

using MimironSQL.EntityFrameworkCore.Query.Internal.Visitor;
using MimironSQL.EntityFrameworkCore.Query.Internal.Expressions;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

internal sealed class MimironDb2ShapedQueryCompilingExpressionVisitor(
    ShapedQueryCompilingExpressionVisitorDependencies dependencies,
    QueryCompilationContext queryCompilationContext)
    : ShapedQueryCompilingExpressionVisitor(dependencies, queryCompilationContext)
{
    private static readonly MethodInfo TableMethodInfo = typeof(Db2TableEnumerator)
        .GetTypeInfo()
        .GetDeclaredMethod(nameof(Db2TableEnumerator.Table))!;

    private static readonly MethodInfo QueryMethodInfo = typeof(Db2ClientQueryExecutor)
        .GetTypeInfo()
        .GetDeclaredMethods(nameof(Db2ClientQueryExecutor.Query))
        .Single(m => m.IsGenericMethodDefinition && m.GetParameters().Length == 8);

    private static readonly MethodInfo EnumerableSingleMethodInfo = typeof(Enumerable)
        .GetTypeInfo()
        .GetDeclaredMethods(nameof(Enumerable.Single))
        .Single(m => m.GetParameters().Length == 1);

    private static readonly MethodInfo EnumerableSingleOrDefaultMethodInfo = typeof(Enumerable)
        .GetTypeInfo()
        .GetDeclaredMethods(nameof(Enumerable.SingleOrDefault))
        .Single(m => m.GetParameters().Length == 1);

    private static readonly MethodInfo EnumerableLastMethodInfo = typeof(Enumerable)
        .GetTypeInfo()
        .GetDeclaredMethods(nameof(Enumerable.Last))
        .Single(m => m.GetParameters().Length == 1);

    private static readonly MethodInfo EnumerableLastOrDefaultMethodInfo = typeof(Enumerable)
        .GetTypeInfo()
        .GetDeclaredMethods(nameof(Enumerable.LastOrDefault))
        .Single(m => m.GetParameters().Length == 1);

    private static readonly MethodInfo GetQueryContextIntParameterValueMethodInfo = typeof(Db2QueryContextParameterReader)
        .GetTypeInfo()
        .GetDeclaredMethod(nameof(Db2QueryContextParameterReader.GetIntParameterValue))!;

    protected override Expression VisitExtension(Expression extensionExpression)
    {
        if (extensionExpression is ShapedQueryExpression shapedQueryExpression)
        {
            // EF Core's base visitor expects ShapedQueryExpression.Type to be a sequence,
            // but for terminal operators (e.g., First/Single) it's a scalar. Handle it here.
            return VisitShapedQuery(shapedQueryExpression);
        }

        if (extensionExpression is Db2QueryExpression db2QueryExpression)
        {
            return Expression.Call(
                TableMethodInfo,
                QueryCompilationContext.QueryContextParameter,
                Expression.Constant(db2QueryExpression));
        }

        return base.VisitExtension(extensionExpression);
    }

    protected override Expression VisitShapedQuery(ShapedQueryExpression shapedQueryExpression)
    {
        ArgumentNullException.ThrowIfNull(shapedQueryExpression);

        if (shapedQueryExpression.QueryExpression is not Db2QueryExpression db2QueryExpression)
        {
            throw new NotSupportedException(
                "MimironDb2 shaped query compilation currently requires Db2QueryExpression as the query root.");
        }

        // Execute Db2QueryExpression -> IEnumerable<ValueBuffer>.
        var valueBuffers = Visit(db2QueryExpression);

        // Shaper compilation:
        // - Inject entity materializers (EF Core transforms StructuralTypeShaperExpression into executable materialization).
        // - Replace ProjectionBindingExpression with a ValueBuffer parameter.
        var valueBufferParameter = Expression.Parameter(typeof(ValueBuffer), "valueBuffer");

        var shaperBody = shapedQueryExpression.ShaperExpression;
        shaperBody = InjectStructuralTypeMaterializers(shaperBody);
        shaperBody = new ProjectionBindingRemovingVisitor(valueBufferParameter).Visit(shaperBody);

        var includeVisitor = new IncludeExpressionExtractingVisitor();
        shaperBody = includeVisitor.Visit(shaperBody);
        var includePlans = includeVisitor.IncludePlans.ToArray();

        var entityShaperBody = (Expression)new StructuralTypeShaperExpression(
            db2QueryExpression.EntityType,
            new ProjectionBindingExpression(db2QueryExpression, new ProjectionMember(), typeof(ValueBuffer)),
            nullable: false);
        entityShaperBody = InjectStructuralTypeMaterializers(entityShaperBody);
        entityShaperBody = new ProjectionBindingRemovingVisitor(valueBufferParameter).Visit(entityShaperBody);
        var entityShaperDelegateType = typeof(Func<,,>).MakeGenericType(typeof(QueryContext), typeof(ValueBuffer), typeof(object));
        var entityShaperLambda = Expression.Lambda(
            entityShaperDelegateType,
            Expression.Convert(entityShaperBody, typeof(object)),
            QueryCompilationContext.QueryContextParameter,
            valueBufferParameter);
        var entityShaperConstant = Expression.Constant(entityShaperLambda.Compile(), entityShaperDelegateType);

        if (db2QueryExpression.Joins.Count > 0)
        {
            var propertyOffsets = ComputeJoinValueBufferPropertyOffsets(db2QueryExpression);
            if (propertyOffsets.Count > 0)
            {
                shaperBody = new JoinValueBufferIndexOffsetVisitor(propertyOffsets).Visit(shaperBody);
            }
        }

        var resultType = shaperBody.Type;
        var shaperDelegateType = typeof(Func<,,>).MakeGenericType(typeof(QueryContext), typeof(ValueBuffer), resultType);
        var shaperLambda = Expression.Lambda(
            shaperDelegateType,
            shaperBody,
            QueryCompilationContext.QueryContextParameter,
            valueBufferParameter);

        var shaperConstant = Expression.Constant(shaperLambda.Compile(), shaperDelegateType);

        // IMPORTANT: Take() counts are often represented as QueryParameterExpression.
        // If we hide the limit inside a constant Db2QueryExpression instance, EF Core cannot see the parameter
        // and won't populate its runtime value. Pass the limit expression into Query(...) as a normal int
        // expression-tree argument so EF's parameterization pipeline can do its job.
        var remainingExpression = RewriteLimitExpression(db2QueryExpression.Limit);
        var offsetExpression = RewriteOffsetExpression(db2QueryExpression.Offset);

        // Sync-only execution for now.
        // TODO: add async query execution support.
        var enumerable = Expression.Call(
            QueryMethodInfo.MakeGenericMethod(resultType),
            QueryCompilationContext.QueryContextParameter,
            Expression.Constant(db2QueryExpression),
            offsetExpression,
            remainingExpression,
            valueBuffers,
            shaperConstant,
            entityShaperConstant,
            Expression.Constant(includePlans, typeof(IncludePlan[])));

        if (db2QueryExpression.TerminalOperator != Db2QueryExpression.Db2TerminalOperator.None)
        {
            Expression terminalResult = db2QueryExpression.TerminalOperator switch
            {
                Db2QueryExpression.Db2TerminalOperator.Count => Expression.Call(
                    typeof(Enumerable)
                        .GetMethods(BindingFlags.Public | BindingFlags.Static)
                        .Single(m => m.Name == nameof(Enumerable.Count) && m.GetParameters().Length == 1)
                        .MakeGenericMethod(resultType),
                    enumerable),
                Db2QueryExpression.Db2TerminalOperator.Last => Expression.Call(
                    EnumerableLastMethodInfo.MakeGenericMethod(resultType),
                    enumerable),
                Db2QueryExpression.Db2TerminalOperator.LastOrDefault => Expression.Call(
                    EnumerableLastOrDefaultMethodInfo.MakeGenericMethod(resultType),
                    enumerable),
                _ => throw new NotSupportedException(
                    $"MimironDb2 does not support terminal operator '{db2QueryExpression.TerminalOperator}'."),
            };

            if (db2QueryExpression.NegateScalarResult)
            {
                if (terminalResult.Type != typeof(bool))
                {
                    throw new NotSupportedException("MimironDb2 scalar negation is only supported for boolean scalar results.");
                }

                terminalResult = Expression.Not(terminalResult);
            }

            return terminalResult;
        }

        // This override bypasses EF Core's base cardinality handling, so we must apply it here.
        Expression result = shapedQueryExpression.ResultCardinality switch
        {
            ResultCardinality.Enumerable => enumerable,
            ResultCardinality.Single => Expression.Call(
                EnumerableSingleMethodInfo.MakeGenericMethod(resultType),
                enumerable),
            ResultCardinality.SingleOrDefault => Expression.Call(
                EnumerableSingleOrDefaultMethodInfo.MakeGenericMethod(resultType),
                enumerable),
            _ => throw new NotSupportedException($"MimironDb2 does not support result cardinality '{shapedQueryExpression.ResultCardinality}'."),
        };

        if (db2QueryExpression.NegateScalarResult)
        {
            if (result.Type != typeof(bool))
            {
                throw new NotSupportedException("MimironDb2 scalar negation is only supported for boolean scalar results.");
            }

            result = Expression.Not(result);
        }

        return result;
    }

    private Expression RewriteLimitExpression(Expression? limit)
    {
        // Goal: produce an expression of type int which is safe and fully reducible/compilable.
        // Specifically, eliminate EF Core extension nodes like QueryParameterExpression by rewriting them
        // into a normal method call which reads the parameter value from QueryContext at runtime.

        if (limit is null)
        {
            return Expression.Constant(-1);
        }

        return Rewrite(limit);

        Expression Rewrite(Expression expression)
        {
            while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            {
                expression = u.Operand;
            }

            if (expression.Type != typeof(int))
            {
                throw new NotSupportedException($"MimironDb2 Take() limit expression must be int, but was '{expression.Type.FullName}'.");
            }

            if (expression is ConstantExpression { Value: int })
            {
                return expression;
            }

            // Allow captured variables (closure field/property). These are reducible and safe.
            if (expression is MemberExpression)
            {
                return expression;
            }

            if (expression is MethodCallExpression call
                && call.Method.DeclaringType == typeof(Math)
                && call.Arguments.Count == 2
                && (call.Method.Name == nameof(Math.Min) || call.Method.Name == nameof(Math.Max))
                && call.Method.ReturnType == typeof(int))
            {
                return Expression.Call(call.Method, Rewrite(call.Arguments[0]), Rewrite(call.Arguments[1]));
            }

            // EF Core parameter for Take() count.
            if (expression.GetType().Name == "QueryParameterExpression")
            {
                const BindingFlags InstanceAnyVisibility = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

                var nameProperty = expression.GetType().GetProperty("Name", InstanceAnyVisibility);
                var nameField = expression.GetType().GetField("_name", InstanceAnyVisibility);
                var parameterName = nameProperty?.GetValue(expression) as string
                    ?? nameField?.GetValue(expression) as string;

                if (string.IsNullOrWhiteSpace(parameterName))
                {
                    throw new NotSupportedException("MimironDb2 could not read QueryParameterExpression parameter name.");
                }

                return BuildQueryContextIntParameterLookup(parameterName);
            }

            throw new NotSupportedException(
                $"MimironDb2 only supports Take() limits which are constants, captured variables, QueryParameterExpression, or Math.Min/Max compositions during bootstrap. Limit expression type: {expression.GetType().FullName}. Expression: {expression}.");
        }
    }

    private Expression RewriteOffsetExpression(Expression? offset)
    {
        // Goal: same as RewriteLimitExpression, but for Skip() offsets.
        // Support addition compositions since multiple Skip() calls are cumulative.

        if (offset is null)
        {
            return Expression.Constant(0);
        }

        return Rewrite(offset);

        Expression Rewrite(Expression expression)
        {
            while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            {
                expression = u.Operand;
            }

            if (expression.Type != typeof(int))
            {
                throw new NotSupportedException($"MimironDb2 Skip() offset expression must be int, but was '{expression.Type.FullName}'.");
            }

            if (expression is ConstantExpression { Value: int })
            {
                return expression;
            }

            // Allow captured variables (closure field/property). These are reducible and safe.
            if (expression is MemberExpression)
            {
                return expression;
            }

            if (expression is BinaryExpression { NodeType: ExpressionType.Add or ExpressionType.AddChecked } add)
            {
                return Expression.Add(Rewrite(add.Left), Rewrite(add.Right));
            }

            if (expression is MethodCallExpression call
                && call.Method.DeclaringType == typeof(Math)
                && call.Arguments.Count == 2
                && (call.Method.Name == nameof(Math.Min) || call.Method.Name == nameof(Math.Max))
                && call.Method.ReturnType == typeof(int))
            {
                return Expression.Call(call.Method, Rewrite(call.Arguments[0]), Rewrite(call.Arguments[1]));
            }

            // EF Core parameter for Skip() count.
            if (expression.GetType().Name == "QueryParameterExpression")
            {
                const BindingFlags InstanceAnyVisibility = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

                var nameProperty = expression.GetType().GetProperty("Name", InstanceAnyVisibility);
                var nameField = expression.GetType().GetField("_name", InstanceAnyVisibility);
                var parameterName = nameProperty?.GetValue(expression) as string
                    ?? nameField?.GetValue(expression) as string;

                if (string.IsNullOrWhiteSpace(parameterName))
                {
                    throw new NotSupportedException("MimironDb2 could not read QueryParameterExpression parameter name.");
                }

                return BuildQueryContextIntParameterLookup(parameterName);
            }

            throw new NotSupportedException(
                $"MimironDb2 only supports Skip() offsets which are constants, captured variables, QueryParameterExpression, or simple Add/Min/Max compositions during bootstrap. Offset expression type: {expression.GetType().FullName}. Expression: {expression}.");
        }
    }

    private Expression BuildQueryContextIntParameterLookup(string parameterName)
    {
        return Expression.Call(
            GetQueryContextIntParameterValueMethodInfo,
            QueryCompilationContext.QueryContextParameter,
            Expression.Constant(parameterName));
    }

    private static Dictionary<IPropertyBase, int> ComputeJoinValueBufferPropertyOffsets(Db2QueryExpression queryExpression)
    {
        ArgumentNullException.ThrowIfNull(queryExpression);

        static int GetEntityValueBufferLength(IEntityType et)
            => !et.GetProperties().Any() ? 0 : et.GetProperties().Max(static p => p.GetIndex()) + 1;

        var offsets = new Dictionary<IPropertyBase, int>();

        var runningOffset = GetEntityValueBufferLength(queryExpression.EntityType);
        for (var joinIndex = 0; joinIndex < queryExpression.Joins.Count; joinIndex++)
        {
            var innerEntityType = queryExpression.Joins[joinIndex].Inner.EntityType;
            var innerOffset = runningOffset;

            // Map all inner properties to this slot offset.
            foreach (var p in innerEntityType.GetProperties())
            {
                offsets[p] = innerOffset;
            }

            runningOffset += GetEntityValueBufferLength(innerEntityType);
        }

        return offsets;
    }
}