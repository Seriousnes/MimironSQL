using System.Linq.Expressions;

namespace MimironSQL.Db2.Query;

internal enum Db2FinalOperator
{
    None = 0,
    First,
    FirstOrDefault,
    Any,
    All,
    Count,
    Single,
    SingleOrDefault,
}

internal abstract record Db2QueryOperation;

internal sealed record Db2WhereOperation(LambdaExpression Predicate) : Db2QueryOperation;

internal sealed record Db2SelectOperation(LambdaExpression Selector) : Db2QueryOperation;

internal sealed record Db2TakeOperation(int Count) : Db2QueryOperation;

internal sealed record Db2IncludeOperation(LambdaExpression Navigation) : Db2QueryOperation;

internal sealed record Db2QueryPipeline(
    Expression OriginalExpression,
    Expression ExpressionWithoutFinalOperator,
    IReadOnlyList<Db2QueryOperation> Operations,
    Db2FinalOperator FinalOperator,
    Type FinalElementType,
    LambdaExpression? FinalPredicate)
{
    public static Db2QueryPipeline Parse(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        var opsReversed = new List<Db2QueryOperation>();
        var finalOperator = Db2FinalOperator.None;
        var finalElementType = GetSequenceElementType(expression.Type) ?? expression.Type;
        LambdaExpression? finalPredicate = null;

        var expressionWithoutFinal = expression;
        if (expression is MethodCallExpression { Method.DeclaringType: { } declaring } m0 && declaring == typeof(Queryable))
        {
            if (m0.Method.Name is nameof(Queryable.First) or nameof(Queryable.FirstOrDefault) or nameof(Queryable.Any) or nameof(Queryable.All) or nameof(Queryable.Count) or nameof(Queryable.Single) or nameof(Queryable.SingleOrDefault))
            {
                finalOperator = m0.Method.Name switch
                {
                    nameof(Queryable.First) => Db2FinalOperator.First,
                    nameof(Queryable.FirstOrDefault) => Db2FinalOperator.FirstOrDefault,
                    nameof(Queryable.Any) => Db2FinalOperator.Any,
                    nameof(Queryable.All) => Db2FinalOperator.All,
                    nameof(Queryable.Count) => Db2FinalOperator.Count,
                    nameof(Queryable.Single) => Db2FinalOperator.Single,
                    nameof(Queryable.SingleOrDefault) => Db2FinalOperator.SingleOrDefault,
                    _ => Db2FinalOperator.None,
                };

                finalElementType = m0.Method.ReturnType;
                expressionWithoutFinal = m0.Arguments[0];

                if (m0.Arguments is { Count: 2 })
                {
                    finalPredicate = UnquoteLambda(m0.Arguments[1]);

                    switch (finalOperator)
                    {
                        case Db2FinalOperator.All:
                            break;
                        default:
                            opsReversed.Add(new Db2WhereOperation(finalPredicate));
                            break;
                    }
                }
            }
        }

        var current = expressionWithoutFinal;

        while (current is MethodCallExpression m)
        {
            var name = m.Method.Name;

            if (m.Method.DeclaringType == typeof(Queryable))
            {
                switch (name)
                {
                    case nameof(Queryable.First) or nameof(Queryable.FirstOrDefault) or nameof(Queryable.Any) or nameof(Queryable.All) or nameof(Queryable.Count) or nameof(Queryable.Single) or nameof(Queryable.SingleOrDefault):
                        throw new NotSupportedException($"{name} must be the terminal operator for this provider.");
                    case nameof(Queryable.Where):
                        {
                            var predicate = UnquoteLambda(m.Arguments[1]);
                            opsReversed.Add(new Db2WhereOperation(predicate));
                            current = m.Arguments[0];
                            continue;
                        }

                    case nameof(Queryable.Select):
                        {
                            var selector = UnquoteLambda(m.Arguments[1]);
                            opsReversed.Add(new Db2SelectOperation(selector));
                            current = m.Arguments[0];
                            continue;
                        }

                    case nameof(Queryable.Take):
                        {
                            if (m.Arguments[1] is not ConstantExpression c || c.Value is not int count)
                                throw new NotSupportedException("Queryable.Take requires a constant integer count for this provider.");

                            opsReversed.Add(new Db2TakeOperation(count));
                            current = m.Arguments[0];
                            continue;
                        }

                    default:
                        throw new NotSupportedException($"Unsupported Queryable operator: {m.Method.Name}.");
                }
            }

            if (m.Method.DeclaringType == typeof(Db2QueryableExtensions) && name == nameof(Db2QueryableExtensions.Include))
            {
                var navigation = UnquoteLambda(m.Arguments[1]);
                opsReversed.Add(new Db2IncludeOperation(navigation));
                current = m.Arguments[0];
                continue;
            }

            throw new NotSupportedException($"Unsupported operator: {m.Method.DeclaringType?.FullName ?? "<unknown>"}.{m.Method.Name}.");
        }

        opsReversed.Reverse();

        return new Db2QueryPipeline(
            OriginalExpression: expression,
            ExpressionWithoutFinalOperator: expressionWithoutFinal,
            Operations: opsReversed,
            FinalOperator: finalOperator,
            FinalElementType: finalElementType,
            FinalPredicate: finalPredicate);
    }

    private static LambdaExpression UnquoteLambda(Expression expression)
    {
        return expression switch
        {
            UnaryExpression { NodeType: ExpressionType.Quote, Operand: LambdaExpression l } => l,
            LambdaExpression l2 => l2,
            _ => throw new NotSupportedException("Expected a quoted lambda expression."),
        };
    }

    private static Type? GetSequenceElementType(Type sequenceType)
        => sequenceType.IsGenericType && sequenceType.GetGenericTypeDefinition() == typeof(IEnumerable<>)
            ? sequenceType.GetGenericArguments()[0]
            : sequenceType.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                ?.GetGenericArguments()[0];
}
