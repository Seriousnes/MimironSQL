using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore.Query;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

/// <summary>
/// DB2-specific query translation preprocessor.
/// Strips Include/ThenInclude calls and stores them for post-materialization application
/// via <see cref="Db2IncludeChainExecutor"/>. This prevents EF Core's
/// <c>NavigationExpandingExpressionVisitor</c> from expanding includes into joins
/// that this provider cannot handle.
/// </summary>
/// <remarks>
/// <para>
/// The preprocessing strategy handles both explicit Include() calls and AutoInclude navigations:
/// </para>
/// <list type="number">
///   <item>Strip explicit Include/ThenInclude method calls BEFORE base.Process() and capture them</item>
///   <item>Keep IgnoreAutoIncludes() in the expression so EF knows not to add auto-includes</item>
///   <item>Call base.Process() - EF's NavigationExpandingExpressionVisitor may add IncludeExpression nodes for auto-includes</item>
///   <item>Strip any IncludeExpression nodes AFTER base.Process() and capture them</item>
///   <item>Combine explicit + auto-include chains and store for later application</item>
/// </list>
/// </remarks>
internal sealed class Db2QueryTranslationPreprocessor(
    QueryTranslationPreprocessorDependencies dependencies,
    QueryCompilationContext queryCompilationContext)
    : QueryTranslationPreprocessor(dependencies, queryCompilationContext)
{
    /// <summary>
    /// Preprocesses the query expression, stripping Include/ThenInclude calls
    /// before passing to the base implementation.
    /// </summary>
    public override Expression Process(Expression query)
    {
        // Step 1: Strip explicit Include/ThenInclude method calls AND most EF modifiers BEFORE base.Process().
        // Keep IgnoreAutoIncludes() in the expression so EF can honor it when expanding auto-includes.
        var preprocessed = Db2ExpressionPreprocessor.Preprocess(query, stripIgnoreAutoIncludes: false);

        // Verify the cleaned expression has no remaining Include method calls
        if (ContainsIncludeCalls(preprocessed.CleanedExpression))
        {
            throw new InvalidOperationException("Include calls were not properly stripped. This is a bug.");
        }

        // Step 2: Call base.Process() - EF may add IncludeExpression nodes for auto-includes.
        var processed = base.Process(preprocessed.CleanedExpression);

        // Step 3: Strip any IncludeExpression nodes EF added (for auto-includes)
        // If IgnoreAutoIncludes was specified, don't capture auto-includes (they'll be discarded)
        List<MemberInfo[]> autoIncludeChains = [];
        Expression cleanedProcessed = processed;

        if (ContainsIncludeExpressions(processed))
        {
            var (stripped, chains) = StripIncludeExpressions(processed);
            cleanedProcessed = stripped;

            // Only keep auto-include chains if user didn't request to ignore them
            if (!preprocessed.IgnoreAutoIncludes)
            {
                autoIncludeChains = chains;
            }
        }

        // Step 4: Combine explicit + auto-include chains
        var allIncludeChains = CombineIncludeChains(preprocessed.IncludeChains, autoIncludeChains);

        // Store combined include chains for retrieval during compilation
        if (allIncludeChains.Count > 0)
        {
            var rootEntityType = TryGetRootEntityType(preprocessed.CleanedExpression);
            if (rootEntityType is not null)
            {
                Db2IncludeStorage.Store(rootEntityType, allIncludeChains, preprocessed.IgnoreAutoIncludes);
            }
        }

        return cleanedProcessed;
    }

    /// <summary>
    /// Checks if the expression tree contains any IncludeExpression nodes.
    /// </summary>
    private static bool ContainsIncludeExpressions(Expression expression)
    {
        if (IncludeExpressionStripper.IncludeExpressionType is null)
            return false;

        var checker = new IncludeExpressionChecker();
        checker.Visit(expression);
        return checker.FoundInclude;
    }

    private sealed class IncludeExpressionChecker : ExpressionVisitor
    {
        public bool FoundInclude { get; private set; }

        public override Expression? Visit(Expression? node)
        {
            if (FoundInclude || node is null)
                return node; // Short-circuit

            if (IncludeExpressionStripper.IncludeExpressionType!.IsInstanceOfType(node))
            {
                FoundInclude = true;
                return node;
            }

            return base.Visit(node);
        }
    }

    /// <summary>
    /// Strips IncludeExpression nodes from the expression tree (added by EF for auto-includes).
    /// </summary>
    private static (Expression Cleaned, List<MemberInfo[]> IncludeChains) StripIncludeExpressions(Expression expression)
    {
        var visitor = new IncludeExpressionStripper();
        var cleaned = visitor.Visit(expression) ?? expression;
        return (cleaned, visitor.IncludeChains);
    }

    /// <summary>
    /// Combines explicit include chains with auto-include chains, avoiding duplicates.
    /// </summary>
    private static IReadOnlyList<MemberInfo[]> CombineIncludeChains(
        IReadOnlyList<MemberInfo[]> explicitChains,
        List<MemberInfo[]> autoIncludeChains)
    {
        if (autoIncludeChains.Count == 0)
            return explicitChains;

        if (explicitChains.Count == 0)
            return autoIncludeChains;

        // Create a set of explicit chain signatures to avoid duplicates
        var explicitSet = new HashSet<string>(explicitChains.Select(ChainSignature));

        var combined = new List<MemberInfo[]>(explicitChains);
        foreach (var autoChain in autoIncludeChains)
        {
            var signature = ChainSignature(autoChain);
            if (!explicitSet.Contains(signature))
            {
                combined.Add(autoChain);
                explicitSet.Add(signature);
            }
        }

        return combined;

        static string ChainSignature(MemberInfo[] chain)
            => string.Join(".", chain.Select(static m => m.DeclaringType?.FullName + "." + m.Name));
    }

    /// <summary>
    /// Visitor that strips IncludeExpression nodes and extracts the navigation chains.
    /// EF Core's IncludeExpression wraps the source expression and contains navigation info.
    /// </summary>
    private sealed class IncludeExpressionStripper : ExpressionVisitor
    {
        // EF Core's IncludeExpression type (resolved at runtime to avoid assembly coupling)
        internal static readonly Type? IncludeExpressionType = Type.GetType(
            "Microsoft.EntityFrameworkCore.Query.IncludeExpression, Microsoft.EntityFrameworkCore");

        public List<MemberInfo[]> IncludeChains { get; } = [];

        public override Expression? Visit(Expression? node)
        {
            if (node is null)
                return null;

            // Check if this is an IncludeExpression
            if (IncludeExpressionType is not null && IncludeExpressionType.IsInstanceOfType(node))
            {
                return VisitIncludeExpression(node);
            }

            return base.Visit(node);
        }

        private Expression VisitIncludeExpression(Expression includeExpr)
        {
            // IncludeExpression has properties:
            // - Expression EntityExpression (the source)
            // - INavigationBase Navigation
            // - LambdaExpression NavigationExpression (optional, for ThenInclude)

            var entityExpressionProp = includeExpr.GetType().GetProperty("EntityExpression");
            var navigationProp = includeExpr.GetType().GetProperty("Navigation");

            if (entityExpressionProp is null || navigationProp is null)
            {
                // Fallback: just visit children
                return base.Visit(includeExpr) ?? includeExpr;
            }

            var entityExpression = (Expression?)entityExpressionProp.GetValue(includeExpr);
            var navigation = navigationProp.GetValue(includeExpr);

            // Extract the navigation member
            var propertyInfoProp = navigation?.GetType().GetProperty("PropertyInfo");
            var navPropertyInfo = propertyInfoProp?.GetValue(navigation) as PropertyInfo;

            if (navPropertyInfo is not null)
            {
                // Build the include chain by walking up nested IncludeExpressions
                var chain = new List<MemberInfo> { navPropertyInfo };

                // Check if entityExpression is also an IncludeExpression (ThenInclude case)
                var current = entityExpression;
                while (current is not null && IncludeExpressionType!.IsInstanceOfType(current))
                {
                    var innerEntityProp = current.GetType().GetProperty("EntityExpression");
                    var innerNavProp = current.GetType().GetProperty("Navigation");
                    var innerNav = innerNavProp?.GetValue(current);
                    var innerNavInfo = innerNav?.GetType().GetProperty("PropertyInfo")?.GetValue(innerNav) as PropertyInfo;

                    if (innerNavInfo is not null)
                        chain.Insert(0, innerNavInfo);

                    current = innerEntityProp?.GetValue(current) as Expression;
                }

                IncludeChains.Add([.. chain]);
            }

            // Visit the source expression (unwrap the include)
            if (entityExpression is not null)
                return Visit(entityExpression) ?? entityExpression;

            return includeExpr;
        }
    }

    /// <summary>
    /// Checks if the expression tree contains any Include/ThenInclude calls.
    /// </summary>
    private static bool ContainsIncludeCalls(Expression expression)
    {
        var checker = new IncludeCallChecker();
        checker.Visit(expression);
        return checker.FoundInclude;
    }

    private sealed class IncludeCallChecker : ExpressionVisitor
    {
        public bool FoundInclude { get; private set; }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (Db2ExpressionPreprocessor.LooksLikeEfIncludeMethod(node.Method))
            {
                FoundInclude = true;
                return node; // Short-circuit
            }
            return base.VisitMethodCall(node);
        }
    }

    /// <summary>
    /// Attempts to extract the root entity type from the query expression.
    /// </summary>
    private static Type? TryGetRootEntityType(Expression expression)
    {
        var current = expression;

        while (current is MethodCallExpression methodCall)
        {
            // Most LINQ methods have the source as argument[0]
            if (methodCall.Arguments.Count > 0)
            {
                current = methodCall.Arguments[0];
            }
            else if (methodCall.Object is not null)
            {
                current = methodCall.Object;
            }
            else
            {
                break;
            }
        }

        // Look for IQueryable<TEntity> or DbSet<TEntity>
        if (current is ConstantExpression { Value: IQueryable queryable })
        {
            var elementType = queryable.ElementType;
            if (elementType is not null && !elementType.IsValueType && elementType != typeof(string))
            {
                return elementType;
            }
        }

        // Try to extract from the expression type
        var type = current.Type;
        if (type.IsGenericType)
        {
            var genericDef = type.GetGenericTypeDefinition();
            if (genericDef == typeof(IQueryable<>) || genericDef == typeof(IEnumerable<>))
            {
                return type.GetGenericArguments()[0];
            }
        }

        return null;
    }
}
