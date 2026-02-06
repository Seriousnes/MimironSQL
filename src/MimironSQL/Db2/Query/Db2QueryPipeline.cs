using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using System.Reflection;

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

internal sealed record Db2SkipOperation(int Count) : Db2QueryOperation;

internal sealed record Db2IncludeOperation(IReadOnlyList<MemberInfo> Members) : Db2QueryOperation;

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

                    case nameof(Queryable.Skip):
                        {
                            if (m.Arguments[1] is not ConstantExpression c || c.Value is not int count)
                                throw new NotSupportedException("Queryable.Skip requires a constant integer count for this provider.");

                            opsReversed.Add(new Db2SkipOperation(count));
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

                var members = ParseMemberChain(navigation);
                opsReversed.Add(new Db2IncludeOperation(members));
                current = m.Arguments[0];
                continue;
            }

            if (m.Method.DeclaringType == typeof(EntityFrameworkQueryableExtensions) &&
                name is nameof(EntityFrameworkQueryableExtensions.Include) or nameof(EntityFrameworkQueryableExtensions.ThenInclude))
            {
                var (source, members) = ExtractEfIncludeChain(m);
                opsReversed.Add(new Db2IncludeOperation(members));
                current = source;
                continue;
            }

            if (m.Method.DeclaringType == typeof(EntityFrameworkQueryableExtensions) &&
                name is nameof(EntityFrameworkQueryableExtensions.AsNoTracking) or
                       nameof(EntityFrameworkQueryableExtensions.AsNoTrackingWithIdentityResolution) or
                       nameof(EntityFrameworkQueryableExtensions.AsTracking) or
                       nameof(EntityFrameworkQueryableExtensions.IgnoreAutoIncludes) or
                       nameof(EntityFrameworkQueryableExtensions.IgnoreQueryFilters) or
                       nameof(EntityFrameworkQueryableExtensions.TagWith) or
                       nameof(EntityFrameworkQueryableExtensions.TagWithCallSite))
            {
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

    private static MemberInfo[] ParseMemberChain(LambdaExpression navigation)
    {
        ArgumentNullException.ThrowIfNull(navigation);

        if (navigation.Parameters is not { Count: 1 })
            throw new NotSupportedException("Include navigation must be a lambda with a single parameter.");

        if (navigation.ReturnType.IsValueType)
            throw new NotSupportedException("Include only supports reference-type navigations.");

        var rootParam = navigation.Parameters[0];

        var body = navigation.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked })
            throw new NotSupportedException("Include does not support casts; use direct member access.");

        var membersReversed = new List<MemberInfo>();
        var current = body;

        while (current is MemberExpression { Member: PropertyInfo or FieldInfo } member)
        {
            membersReversed.Add(member.Member);
            current = member.Expression;
        }

        if (current != rootParam)
            throw new NotSupportedException("Include only supports member access chains rooted at the lambda parameter.");

        membersReversed.Reverse();
        return [.. membersReversed];
    }

    private static (Expression Source, MemberInfo[] Members) ExtractEfIncludeChain(MethodCallExpression call)
    {
        if (call.Method.DeclaringType != typeof(EntityFrameworkQueryableExtensions))
            throw new InvalidOperationException("Expected EF Core Include/ThenInclude method call.");

        if (call.Method.Name == nameof(EntityFrameworkQueryableExtensions.Include))
        {
            var navigation = UnquoteLambda(call.Arguments[1]);
            var members = ParseMemberChain(navigation);
            return (call.Arguments[0], members);
        }

        if (call.Method.Name == nameof(EntityFrameworkQueryableExtensions.ThenInclude))
        {
            var (source, previousMembers) = ExtractEfIncludeChain((MethodCallExpression)call.Arguments[0]);
            var navigation = UnquoteLambda(call.Arguments[1]);
            var members = ParseMemberChain(navigation);

            return (source, [.. previousMembers, .. members]);
        }

        throw new InvalidOperationException($"Unexpected EF include method: {call.Method.Name}");
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
