using System.Linq.Expressions;
using System.Reflection;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

/// <summary>
/// Result of preprocessing an EF Core expression tree.
/// Contains a cleaned expression (with Include/ThenInclude and EF query modifiers stripped)
/// along with metadata extracted during preprocessing.
/// </summary>
internal sealed record Db2PreprocessedExpression(
    Expression CleanedExpression,
    IReadOnlyList<MemberInfo[]> IncludeChains,
    bool IgnoreAutoIncludes);

/// <summary>
/// Preprocesses an EF Core LINQ expression tree by stripping
/// Include/ThenInclude and EF query modifiers (AsNoTracking, etc.),
/// collecting the include chain metadata for later application.
/// </summary>
internal static class Db2ExpressionPreprocessor
{
    private static readonly Type IQueryableType = typeof(IQueryable);

    public static Db2PreprocessedExpression Preprocess(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        var visitor = new StripVisitor();
        var cleaned = visitor.Visit(expression);
        return new Db2PreprocessedExpression(cleaned, visitor.IncludeChains, visitor.IgnoreAutoIncludes);
    }

    /// <summary>
    /// Strips Include/ThenInclude and EF query modifier calls from the expression tree,
    /// replacing them with their source argument. Collects include chain metadata.
    /// </summary>
    private sealed class StripVisitor : ExpressionVisitor
    {
        public List<MemberInfo[]> IncludeChains { get; } = [];
        public bool IgnoreAutoIncludes { get; private set; }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (LooksLikeEfIncludeMethod(node.Method))
            {
                var (source, members) = ExtractEfIncludeChain(node);
                IncludeChains.Add(members);
                return Visit(source);
            }

            if (LooksLikeEfQueryModifierMethod(node.Method))
            {
                if (node.Method.Name == "IgnoreAutoIncludes")
                    IgnoreAutoIncludes = true;

                return Visit(node.Arguments[0]);
            }

            return base.VisitMethodCall(node);
        }
    }

    // ──────── Include detection helpers (moved from Db2QueryPipeline) ────────

    internal static bool LooksLikeEfIncludeMethod(MethodInfo method)
    {
        if (method is not { IsStatic: true })
            return false;

        if (method.Name is not ("Include" or "ThenInclude"))
            return false;

        var parameters = method.GetParameters();
        if (parameters is not { Length: 2 })
            return false;

        if (!IQueryableType.IsAssignableFrom(parameters[0].ParameterType))
            return false;

        return IsExpressionOfLambda(parameters[1].ParameterType);
    }

    internal static bool LooksLikeEfQueryModifierMethod(MethodInfo method)
    {
        if (method is not { IsStatic: true })
            return false;

        if (method.Name is not (
            "AsNoTracking" or
            "AsNoTrackingWithIdentityResolution" or
            "AsTracking" or
            "IgnoreAutoIncludes" or
            "IgnoreQueryFilters" or
            "TagWith" or
            "TagWithCallSite"))
        {
            return false;
        }

        var parameters = method.GetParameters();

        if (method.Name is "TagWith" or "TagWithCallSite")
        {
            return parameters is [{ ParameterType: { } firstParamType }, { ParameterType: { } secondParamType }] &&
                   IQueryableType.IsAssignableFrom(firstParamType) &&
                   secondParamType == typeof(string);
        }

        return parameters is [{ ParameterType: { } onlyParamType }] && IQueryableType.IsAssignableFrom(onlyParamType);
    }

    internal static (Expression Source, MemberInfo[] Members) ExtractEfIncludeChain(MethodCallExpression call)
    {
        if (call.Method.Name == "Include")
        {
            var navigation = UnquoteLambda(call.Arguments[1]);
            var members = ParseMemberChain(navigation);
            return (call.Arguments[0], members);
        }

        if (call.Method.Name == "ThenInclude")
        {
            var (source, previousMembers) = ExtractEfIncludeChain((MethodCallExpression)call.Arguments[0]);
            var navigation = UnquoteLambda(call.Arguments[1]);
            var members = ParseMemberChain(navigation);

            return (source, [.. previousMembers, .. members]);
        }

        throw new InvalidOperationException($"Unexpected EF include method: {call.Method.Name}");
    }

    internal static MemberInfo[] ParseMemberChain(LambdaExpression navigation)
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

    internal static LambdaExpression UnquoteLambda(Expression expression)
    {
        return expression switch
        {
            UnaryExpression { NodeType: ExpressionType.Quote, Operand: LambdaExpression l } => l,
            LambdaExpression l2 => l2,
            _ => throw new NotSupportedException("Expected a quoted lambda expression."),
        };
    }

    private static bool IsExpressionOfLambda(Type type)
    {
        if (!type.IsGenericType)
            return false;

        if (type.GetGenericTypeDefinition() != typeof(Expression<>))
            return false;

        return typeof(Delegate).IsAssignableFrom(type.GetGenericArguments()[0]);
    }
}
