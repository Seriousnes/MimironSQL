using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace MimironSQL.Db2.Query;

internal enum Db2FinalOperator
{
    None = 0,
    FirstOrDefault,
    Any,
    Count,
    Single,
}

internal abstract record Db2QueryOperation;

internal sealed record Db2WhereOperation(LambdaExpression Predicate) : Db2QueryOperation;

internal sealed record Db2SelectOperation(LambdaExpression Selector) : Db2QueryOperation;

internal sealed record Db2TakeOperation(int Count) : Db2QueryOperation;

internal sealed record Db2QueryPipeline(
    Expression OriginalExpression,
    Expression ExpressionWithoutFinalOperator,
    IReadOnlyList<Db2QueryOperation> Operations,
    Db2FinalOperator FinalOperator,
    Type FinalElementType)
{
    public static Db2QueryPipeline Parse(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        var opsReversed = new List<Db2QueryOperation>();
        var finalOperator = Db2FinalOperator.None;
        var finalElementType = GetSequenceElementType(expression.Type) ?? expression.Type;

        var expressionWithoutFinal = expression;
        if (expression is MethodCallExpression { Method.DeclaringType: { } declaring } m0 && declaring == typeof(Queryable))
        {
            if (m0.Method.Name is nameof(Queryable.FirstOrDefault) or nameof(Queryable.Any) or nameof(Queryable.Count) or nameof(Queryable.Single))
            {
                finalOperator = m0.Method.Name switch
                {
                    nameof(Queryable.FirstOrDefault) => Db2FinalOperator.FirstOrDefault,
                    nameof(Queryable.Any) => Db2FinalOperator.Any,
                    nameof(Queryable.Count) => Db2FinalOperator.Count,
                    nameof(Queryable.Single) => Db2FinalOperator.Single,
                    _ => Db2FinalOperator.None,
                };

                finalElementType = m0.Method.ReturnType;
                expressionWithoutFinal = m0.Arguments[0];

                if (m0.Arguments.Count == 2)
                {
                    var finalPredicate = UnquoteLambda(m0.Arguments[1]);
                    opsReversed.Add(new Db2WhereOperation(finalPredicate));
                }
            }
        }

        var current = expressionWithoutFinal;

        while (current is MethodCallExpression m && m.Method.DeclaringType == typeof(Queryable))
        {
            var name = m.Method.Name;

            if (name is nameof(Queryable.FirstOrDefault) or nameof(Queryable.Any) or nameof(Queryable.Count) or nameof(Queryable.Single))
                throw new NotSupportedException($"{name} must be the terminal operator for this provider.");

            if (name == nameof(Queryable.Where))
            {
                var predicate = UnquoteLambda(m.Arguments[1]);
                opsReversed.Add(new Db2WhereOperation(predicate));
                current = m.Arguments[0];
                continue;
            }

            if (name == nameof(Queryable.Select))
            {
                var selector = UnquoteLambda(m.Arguments[1]);
                opsReversed.Add(new Db2SelectOperation(selector));
                current = m.Arguments[0];
                continue;
            }

            if (name == nameof(Queryable.Take))
            {
                if (m.Arguments[1] is not ConstantExpression c || c.Value is not int count)
                    throw new NotSupportedException("Queryable.Take requires a constant integer count for this provider.");

                opsReversed.Add(new Db2TakeOperation(count));
                current = m.Arguments[0];
                continue;
            }

            throw new NotSupportedException($"Unsupported Queryable operator: {m.Method.Name}.");
        }

        opsReversed.Reverse();

        return new Db2QueryPipeline(
            OriginalExpression: expression,
            ExpressionWithoutFinalOperator: expressionWithoutFinal,
            Operations: opsReversed,
            FinalOperator: finalOperator,
            FinalElementType: finalElementType);
    }

    private static LambdaExpression UnquoteLambda(Expression expression)
    {
        if (expression is UnaryExpression { NodeType: ExpressionType.Quote, Operand: LambdaExpression l })
            return l;

        if (expression is LambdaExpression l2)
            return l2;

        throw new NotSupportedException("Expected a quoted lambda expression.");
    }

    private static Type? GetSequenceElementType(Type sequenceType)
        => sequenceType.IsGenericType && sequenceType.GetGenericTypeDefinition() == typeof(IEnumerable<>)
            ? sequenceType.GetGenericArguments()[0]
            : sequenceType.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                ?.GetGenericArguments()[0];
}
