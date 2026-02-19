using System.Linq.Expressions;
using System.Reflection;
using System.Collections.Concurrent;
using System.Text;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

using System.Linq;

using MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.EntityFrameworkCore.Db2.Query.Visitor;
using MimironSQL.EntityFrameworkCore.Infrastructure;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;
using MimironSQL.Providers;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

internal sealed class MimironDb2ShapedQueryCompilingExpressionVisitor(
    ShapedQueryCompilingExpressionVisitorDependencies dependencies,
    QueryCompilationContext queryCompilationContext)
    : ShapedQueryCompilingExpressionVisitor(dependencies, queryCompilationContext)
{
    private sealed record ParameterStoresAccessor(Func<QueryContext, object?>[] Stores);

    private static readonly ConcurrentDictionary<Type, ParameterStoresAccessor> QueryContextParameterStoresAccessorCache = new();

    private static readonly MethodInfo TableMethodInfo = typeof(MimironDb2ShapedQueryCompilingExpressionVisitor)
        .GetTypeInfo()
        .GetDeclaredMethod(nameof(Table))!;

    private static readonly MethodInfo QueryMethodInfo = typeof(MimironDb2ShapedQueryCompilingExpressionVisitor)
        .GetTypeInfo()
        .GetDeclaredMethods(nameof(Query))
        .Single(m => m.IsGenericMethodDefinition && m.GetParameters().Length == 7);

    private static readonly MethodInfo EnumerableSingleMethodInfo = typeof(Enumerable)
        .GetTypeInfo()
        .GetDeclaredMethods(nameof(Enumerable.Single))
        .Single(m => m.GetParameters().Length == 1);

    private static readonly MethodInfo EnumerableSingleOrDefaultMethodInfo = typeof(Enumerable)
        .GetTypeInfo()
        .GetDeclaredMethods(nameof(Enumerable.SingleOrDefault))
        .Single(m => m.GetParameters().Length == 1);

    private static readonly MethodInfo GetQueryContextIntParameterValueMethodInfo = typeof(MimironDb2ShapedQueryCompilingExpressionVisitor)
        .GetTypeInfo()
        .GetDeclaredMethod(nameof(GetQueryContextIntParameterValue))!;

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

        // Sync-only execution for now.
        // TODO: add async query execution support.
        var enumerable = Expression.Call(
            QueryMethodInfo.MakeGenericMethod(resultType),
            QueryCompilationContext.QueryContextParameter,
            Expression.Constant(db2QueryExpression),
            remainingExpression,
            valueBuffers,
            shaperConstant,
            entityShaperConstant,
            Expression.Constant(includePlans, typeof(IncludePlan[])));

        if (db2QueryExpression.TerminalOperator == Db2QueryExpression.Db2TerminalOperator.Count)
        {
            var countMethod = typeof(Enumerable)
                .GetMethods(BindingFlags.Public | BindingFlags.Static)
                .Single(m => m.Name == nameof(Enumerable.Count) && m.GetParameters().Length == 1)
                .MakeGenericMethod(resultType);

            return Expression.Call(countMethod, enumerable);
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
                throw new NotSupportedException("MimironDb2 scalar negation is only supported for boolean scalar results.");

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
            return Expression.Constant(-1);

        return Rewrite(limit);

        Expression Rewrite(Expression expression)
        {
            while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                expression = u.Operand;

            if (expression.Type != typeof(int))
                throw new NotSupportedException($"MimironDb2 Take() limit expression must be int, but was '{expression.Type.FullName}'.");

            if (expression is ConstantExpression { Value: int })
                return expression;

            // Allow captured variables (closure field/property). These are reducible and safe.
            if (expression is MemberExpression)
                return expression;

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
                    throw new NotSupportedException("MimironDb2 could not read QueryParameterExpression parameter name.");

                return BuildQueryContextIntParameterLookup(parameterName);
            }

            throw new NotSupportedException(
                $"MimironDb2 only supports Take() limits which are constants, captured variables, QueryParameterExpression, or Math.Min/Max compositions during bootstrap. Limit expression type: {expression.GetType().FullName}. Expression: {expression}.");
        }
    }

    private Expression BuildQueryContextIntParameterLookup(string parameterName)
    {
        return Expression.Call(
            GetQueryContextIntParameterValueMethodInfo,
            QueryCompilationContext.QueryContextParameter,
            Expression.Constant(parameterName));
    }

    private static int GetQueryContextIntParameterValue(QueryContext queryContext, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(queryContext);
        ArgumentException.ThrowIfNullOrWhiteSpace(parameterName);

        if (!TryGetQueryContextParameterValue(queryContext, parameterName, out var value))
        {
            throw new NotSupportedException(
                $"MimironDb2 could not read parameter '{parameterName}' from QueryContext (runtime type '{queryContext.GetType().FullName}') to evaluate a parameterized Take() limit.");
        }

        if (value is null)
        {
            throw new NotSupportedException(
                $"MimironDb2 parameter '{parameterName}' from QueryContext was null; cannot evaluate a parameterized Take() limit.");
        }

        try
        {
            // Accept common numeric shapes; throw on overflow.
            return value switch
            {
                int i => i,
                byte b => b,
                sbyte sb => sb,
                short s => s,
                ushort us => us,
                long l => checked((int)l),
                ulong ul => checked((int)ul),
                uint ui => checked((int)ui),
                nint ni => checked((int)ni),
                nuint nui => checked((int)nui),
                _ when value is IConvertible => Convert.ToInt32(value, provider: null),
                _ => throw new NotSupportedException(
                    $"MimironDb2 Take() limit parameter '{parameterName}' had unsupported runtime type '{value.GetType().FullName}'.")
            };
        }
        catch (OverflowException ex)
        {
            throw new NotSupportedException(
                $"MimironDb2 Take() limit parameter '{parameterName}' value '{value}' could not be converted to int without overflow.",
                ex);
        }
    }

    private static bool TryGetQueryContextParameterValue(QueryContext queryContext, string parameterName, out object? value)
    {
        var accessor = QueryContextParameterStoresAccessorCache.GetOrAdd(
            queryContext.GetType(),
            static t => new ParameterStoresAccessor(BuildQueryContextParameterStoresAccessors(t).ToArray()));

        foreach (var storeAccessor in accessor.Stores)
        {
            var store = storeAccessor(queryContext);
            if (store is null)
                continue;

            if (TryGetValueFromStore(store, parameterName, out value))
                return true;
        }

        value = null;
        return false;
    }

    private static IEnumerable<Func<QueryContext, object?>> BuildQueryContextParameterStoresAccessors(Type queryContextRuntimeType)
    {
        const BindingFlags InstanceAnyVisibility = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

        foreach (var member in EnumerateCandidateStoreMembers(queryContextRuntimeType, InstanceAnyVisibility))
            yield return BuildMemberAccessor(member);

        static IEnumerable<MemberInfo> EnumerateCandidateStoreMembers(Type t, BindingFlags flags)
        {
            for (var current = t; current is not null; current = current.BaseType)
            {
                foreach (var p in current.GetProperties(flags))
                {
                    if (p.GetIndexParameters().Length != 0)
                        continue;

                    if (LooksLikeStringKeyedDictionary(p.PropertyType))
                        yield return p;
                }

                foreach (var f in current.GetFields(flags))
                {
                    if (LooksLikeStringKeyedDictionary(f.FieldType))
                        yield return f;
                }
            }
        }

        static bool LooksLikeStringKeyedDictionary(Type candidate)
        {
            if (typeof(System.Collections.IDictionary).IsAssignableFrom(candidate))
                return true;

            if (!candidate.IsGenericType && candidate.GetInterfaces().Length == 0)
                return false;

            foreach (var i in candidate.GetInterfaces().Append(candidate))
            {
                if (!i.IsGenericType)
                    continue;

                var def = i.GetGenericTypeDefinition();
                if (def != typeof(IReadOnlyDictionary<,>) && def != typeof(IDictionary<,>))
                    continue;

                var args = i.GetGenericArguments();
                if (args.Length == 2 && args[0] == typeof(string))
                    return true;
            }

            return false;
        }

        static Func<QueryContext, object?> BuildMemberAccessor(MemberInfo member)
        {
            var qc = Expression.Parameter(typeof(QueryContext), "qc");
            var instance = Expression.Convert(qc, member.DeclaringType!);

            Expression access = member switch
            {
                PropertyInfo p => Expression.Property(instance, p),
                FieldInfo f => Expression.Field(instance, f),
                _ => Expression.Constant(null, typeof(object))
            };

            var body = Expression.Convert(access, typeof(object));
            return Expression.Lambda<Func<QueryContext, object?>>(body, qc).Compile();
        }
    }

    private static bool TryGetValueFromStore(object store, string parameterName, out object? value)
    {
        if (store is System.Collections.IDictionary dict)
        {
            if (!dict.Contains(parameterName))
            {
                value = null;
                return false;
            }

            value = dict[parameterName];
            return true;
        }

        // Handle IReadOnlyDictionary<string, object?> and friends via TryGetValue reflection.
        var storeType = store.GetType();
        var tryGetValue = storeType
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .FirstOrDefault(m =>
                m.Name == "TryGetValue"
                && m.GetParameters().Length == 2
                && m.GetParameters()[0].ParameterType == typeof(string)
                && m.GetParameters()[1].ParameterType.IsByRef);

        if (tryGetValue is null)
        {
            value = null;
            return false;
        }

        var args = new object?[] { parameterName, null };
        var ok = (bool)tryGetValue.Invoke(store, args)!;
        value = args[1];
        return ok;
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
                offsets[p] = innerOffset;

            runningOffset += GetEntityValueBufferLength(innerEntityType);
        }

        return offsets;
    }

    private static IEnumerable<TResult> Query<TResult>(
        QueryContext queryContext,
        Db2QueryExpression queryExpression,
        int remaining,
        IEnumerable<ValueBuffer> valueBuffers,
        Func<QueryContext, ValueBuffer, TResult> shaper,
        Func<QueryContext, ValueBuffer, object> entityShaper,
        IncludePlan[] includePlans)
    {
        // Correctness-first: execute client-side for now.
        // TODO: push query operators (filters/limits/projections) into DB2-native execution.
        // TODO: translate predicates during query translation rather than compiling EF Core's rewritten lambdas.
        //       Navigation predicates (Any/Count/Contains over navigations) should become joins/subqueries in the provider IR.

        // Required for tracking queries and include pipelines.
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
                    || TryGetTransparentIdentifierTypes(parameterType, out _, out _))
                {
                    entityPredicates.Add(CompilePredicate(entityType, predicate));
                    continue;
                }

                // Scalar/object projection predicate (e.g., Select(...).First(x => x > 0)).
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

        // Buffering simplifies include execution and avoids triggering additional EF queries.
        // TODO: reintroduce streaming for non-include queries once bootstrap include execution is stable.
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

                if (resultPredicates.Count > 0 && resultPredicates.Any(p => !p(queryContext, boxed)))
                    continue;
            }

            results.Add(result);
            if (remaining > 0)
                remaining--;
        }

        if (includePlans is { Length: > 0 } && results.Count > 0)
            ExecuteIncludes(queryContext, includePlans, results);

        return results;
    }

    private static void ExecuteIncludes<TResult>(QueryContext queryContext, IncludePlan[] includePlans, IReadOnlyList<TResult> results)
    {
        var dbContext = queryContext.Context;

        var store = dbContext.GetService<IMimironDb2Store>();
        var modelBinding = dbContext.GetService<MimironSQL.EntityFrameworkCore.Db2.Model.IDb2ModelBinding>().GetBinding();
        var entityFactory = new DefaultDb2EntityFactory();

        var knownEntities = new List<object>(capacity: results.Count);
        for (var i = 0; i < results.Count; i++)
        {
            var r = results[i];
            if (r is null)
                continue;
            knownEntities.Add((object)r);
        }

        // Cache per-include compiled accessors.
        var getterCache = new Dictionary<(Type ClrType, string Name), Func<object, object?>>(capacity: 16);
        var setterCache = new Dictionary<(Type ClrType, string Name), Action<object, object?>>(capacity: 16);
        var listFactoryCache = new Dictionary<Type, Func<System.Collections.IList>>(capacity: 8);

        // Per-query scan cache to ensure each table is enumerated at most once.
        var oneToManyCache = new Dictionary<(string DependentTable, int ForeignKeyFieldIndex), Dictionary<int, List<object>>>(capacity: 8);

        var traceIncludes = string.Equals(Environment.GetEnvironmentVariable("MIMIRON_TRACE_INCLUDES"), "1", StringComparison.Ordinal);
        if (traceIncludes)
        {
            Console.WriteLine($"[MimironDb2] Includes: extracted {includePlans.Length} plan(s); root results={results.Count}; knownEntities={knownEntities.Count}");
            for (var i = 0; i < includePlans.Length; i++)
            {
                var p = includePlans[i];
                var nav = p.Navigation;
                var kind = nav is ISkipNavigation ? "skip" : nav is INavigation n && n.IsCollection ? "collection" : "reference";
                Console.WriteLine($"[MimironDb2] IncludePlan[{i}]: source={p.SourceClrType.FullName}; nav={nav.DeclaringEntityType.DisplayName()}.{nav.Name} -> {nav.TargetEntityType.DisplayName()} ({kind})");
            }
        }

        // IncludeExpression nodes in the shaper can be discovered in an order where ThenInclude plans
        // appear before their parent Include plan. Since our batched include executor relies on
        // previously materialized source entities (tracked in knownEntities), execute plans in passes
        // until no further progress is made.
        var executed = new bool[includePlans.Length];
        var remaining = includePlans.Length;
        var madeProgress = true;

        while (remaining > 0 && madeProgress)
        {
            madeProgress = false;

            for (var planIndex = 0; planIndex < includePlans.Length; planIndex++)
            {
                if (executed[planIndex])
                    continue;

                var plan = includePlans[planIndex];

                var sources = knownEntities.Where(e => plan.SourceClrType.IsInstanceOfType(e)).ToArray();
                if (sources.Length == 0)
                    continue;

                var knownBefore = knownEntities.Count;
                if (traceIncludes)
                    Console.WriteLine($"[MimironDb2] Executing IncludePlan[{planIndex}] on {sources.Length} source(s); knownEntities={knownBefore}");

                if (plan.Navigation is ISkipNavigation skipNavigation)
                {
                    ExecuteSkipNavigationInclude(dbContext, store, modelBinding, entityFactory, sources, skipNavigation, getterCache, setterCache, listFactoryCache, knownEntities);
                }
                else
                {
                    if (plan.Navigation is not INavigation navigation)
                        throw new NotSupportedException($"MimironDb2 Include navigation type '{plan.Navigation.GetType().FullName}' is not supported.");

                    if (navigation.IsCollection)
                    {
                        ExecuteCollectionNavigationInclude(dbContext, store, modelBinding, entityFactory, sources, navigation, getterCache, setterCache, listFactoryCache, oneToManyCache, knownEntities);
                    }
                    else
                    {
                        ExecuteReferenceNavigationInclude(dbContext, store, modelBinding, entityFactory, sources, navigation, getterCache, setterCache, knownEntities);
                    }
                }

                if (traceIncludes)
                    Console.WriteLine($"[MimironDb2] Executed IncludePlan[{planIndex}]; knownEntities delta={knownEntities.Count - knownBefore}");

                executed[planIndex] = true;
                remaining--;
                madeProgress = true;
            }
        }
    }

    private static void ExecuteSkipNavigationInclude(
        DbContext dbContext,
        IMimironDb2Store store,
        MimironSQL.EntityFrameworkCore.Db2.Model.Db2ModelBinding modelBinding,
        IDb2EntityFactory entityFactory,
        IReadOnlyList<object> sources,
        ISkipNavigation navigation,
        Dictionary<(Type ClrType, string Name), Func<object, object?>> getterCache,
        Dictionary<(Type ClrType, string Name), Action<object, object?>> setterCache,
        Dictionary<Type, Func<System.Collections.IList>> listFactoryCache,
        List<object> knownEntities)
    {
        var traceIncludes = string.Equals(Environment.GetEnvironmentVariable("MIMIRON_TRACE_INCLUDES"), "1", StringComparison.Ordinal);

        var joinEntityType = navigation.JoinEntityType;
        var fkArrayPropertyName = joinEntityType
            .FindAnnotation(Db2ForeignKeyArrayModelRewriter.VirtualForeignKeyArrayPropertyAnnotation)
            ?.Value as string;

        if (string.IsNullOrWhiteSpace(fkArrayPropertyName))
            throw new NotSupportedException($"MimironDb2 could not resolve FK array property for skip navigation '{navigation.Name}'.");

        var sourceClrType = navigation.DeclaringEntityType.ClrType;
        var targetClrType = navigation.TargetEntityType.ClrType;

        var pk = navigation.TargetEntityType.FindPrimaryKey();
        if (pk is null || pk.Properties.Count != 1)
            throw new NotSupportedException($"MimironDb2 skip navigation '{navigation.Name}' target must have a single-column primary key.");

        var targetKeyType = pk.Properties[0].ClrType;
        if (Nullable.GetUnderlyingType(targetKeyType) is { } unwrapped)
            targetKeyType = unwrapped;

        if (targetKeyType != typeof(int))
            throw new NotSupportedException($"MimironDb2 skip navigation '{navigation.Name}' currently supports int keys only (saw '{targetKeyType.FullName}').");

        var fkGetter = GetOrCompileGetter(getterCache, sourceClrType, fkArrayPropertyName);
        var navSetter = GetOrCompileSetter(setterCache, sourceClrType, navigation.Name);
        var listFactory = GetOrCompileListFactory(listFactoryCache, targetClrType);

        var allIds = new HashSet<int>();
        var perSourceIds = new List<int[]>(capacity: sources.Count);

        for (var i = 0; i < sources.Count; i++)
        {
            var source = sources[i];
            var idsObj = fkGetter(source);
            if (idsObj is null)
            {
                perSourceIds.Add([]);
                continue;
            }

            if (idsObj is not System.Collections.IEnumerable idsEnumerable)
                throw new NotSupportedException($"MimironDb2 expected FK array '{fkArrayPropertyName}' to be IEnumerable, but got '{idsObj.GetType().FullName}'.");

            var tmp = new List<int>();
            foreach (var idObj in idsEnumerable)
            {
                if (idObj is null)
                    continue;

                var id = Convert.ToInt32(idObj);
                if (id == 0)
                    continue;

                tmp.Add(id);
                allIds.Add(id);
            }

            perSourceIds.Add(tmp.ToArray());
        }

        if (allIds.Count == 0)
        {
            if (traceIncludes)
                Console.WriteLine($"[MimironDb2] Skip include '{navigation.DeclaringEntityType.DisplayName()}.{navigation.Name}': no FK IDs (fkArrayProperty='{fkArrayPropertyName}')");

            // Still mark empty collections as loaded.
            for (var i = 0; i < sources.Count; i++)
            {
                var empty = listFactory();
                navSetter(sources[i], empty);
                dbContext.Entry(sources[i]).Collection(navigation.Name).IsLoaded = true;
            }

            return;
        }

        var targetTableName = navigation.TargetEntityType.GetTableName() ?? targetClrType.Name;
        var loaded = MaterializeByIdsUntyped(store, modelBinding, entityFactory, targetClrType, targetTableName, [.. allIds], takeCount: null);

        if (traceIncludes)
        {
            var sample = allIds.Take(10).ToArray();
            var min = allIds.Count == 0 ? 0 : allIds.Min();
            var max = allIds.Count == 0 ? 0 : allIds.Max();
            Console.WriteLine($"[MimironDb2] Skip include '{navigation.DeclaringEntityType.DisplayName()}.{navigation.Name}': sources={sources.Count}, ids={allIds.Count}, loaded={loaded.Count}, targetTable={targetTableName}, min={min}, max={max}, sample=[{string.Join(",", sample)}]");
        }

        var byId = new Dictionary<int, object>(capacity: loaded.Count);
        var idGetter = GetOrCompileGetter(getterCache, targetClrType, pk.Properties[0].Name);

        Dictionary<int, object>? trackedById = null;
        if (dbContext.ChangeTracker.QueryTrackingBehavior != QueryTrackingBehavior.NoTracking)
        {
            trackedById = new Dictionary<int, object>();
            foreach (var entry in dbContext.ChangeTracker.Entries())
            {
                var trackedEntity = entry.Entity;
                if (trackedEntity is null || trackedEntity.GetType() != targetClrType)
                    continue;

                var trackedIdObj = idGetter(trackedEntity);
                if (trackedIdObj is null)
                    continue;

                trackedById[Convert.ToInt32(trackedIdObj)] = trackedEntity;
            }
        }

        for (var i = 0; i < loaded.Count; i++)
        {
            var entity = loaded[i];
            var idObj = idGetter(entity);
            if (idObj is null)
                continue;

            var id = Convert.ToInt32(idObj);

            if (byId.ContainsKey(id))
                continue;

            if (trackedById is not null && trackedById.TryGetValue(id, out var tracked))
            {
                byId[id] = tracked;
                if (!knownEntities.Contains(tracked))
                    knownEntities.Add(tracked);
                continue;
            }

            TrackIfNeeded(dbContext, entity);
            knownEntities.Add(entity);
            byId[id] = entity;
        }

        for (var i = 0; i < sources.Count; i++)
        {
            var list = listFactory();
            var ids = perSourceIds[i];
            for (var j = 0; j < ids.Length; j++)
            {
                if (byId.TryGetValue(ids[j], out var entity))
                    list.Add(entity);
            }

            navSetter(sources[i], list);
            dbContext.Entry(sources[i]).Collection(navigation.Name).IsLoaded = true;
        }
    }

    private static void ExecuteReferenceNavigationInclude(
        DbContext dbContext,
        IMimironDb2Store store,
        MimironSQL.EntityFrameworkCore.Db2.Model.Db2ModelBinding modelBinding,
        IDb2EntityFactory entityFactory,
        IReadOnlyList<object> sources,
        INavigation navigation,
        Dictionary<(Type ClrType, string Name), Func<object, object?>> getterCache,
        Dictionary<(Type ClrType, string Name), Action<object, object?>> setterCache,
        List<object> knownEntities)
    {
        var sourceClrType = navigation.DeclaringEntityType.ClrType;
        var targetClrType = navigation.TargetEntityType.ClrType;

        var sourcePk = navigation.DeclaringEntityType.FindPrimaryKey();
        var targetPk = navigation.TargetEntityType.FindPrimaryKey();
        if (sourcePk is null || sourcePk.Properties.Count != 1 || targetPk is null || targetPk.Properties.Count != 1)
            throw new NotSupportedException($"MimironDb2 reference include '{navigation.Name}' requires single-column PKs.");

        var targetKeyType = targetPk.Properties[0].ClrType;
        if (Nullable.GetUnderlyingType(targetKeyType) is { } unwrapped)
            targetKeyType = unwrapped;

        if (targetKeyType != typeof(int))
            throw new NotSupportedException($"MimironDb2 reference include '{navigation.Name}' currently supports int keys only (saw '{targetKeyType.FullName}').");

        // Shared-PK fast path: principal -> dependent reference can be resolved by principal key.
        // For dependent -> principal references, read FK from dependent.
        var ids = new HashSet<int>();
        Func<object, object?> keyGetter;

        if (!navigation.IsOnDependent)
        {
            // Principal to dependent.
            keyGetter = GetOrCompileGetter(getterCache, sourceClrType, sourcePk.Properties[0].Name);
        }
        else
        {
            // Dependent to principal.
            var fkProperty = navigation.ForeignKey.Properties.Single();
            keyGetter = GetOrCompileGetter(getterCache, sourceClrType, fkProperty.Name);
        }

        for (var i = 0; i < sources.Count; i++)
        {
            var keyObj = keyGetter(sources[i]);
            if (keyObj is null)
                continue;
            var id = Convert.ToInt32(keyObj);
            if (id != 0)
                ids.Add(id);
        }

        if (ids.Count == 0)
        {
            for (var i = 0; i < sources.Count; i++)
                dbContext.Entry(sources[i]).Reference(navigation.Name).IsLoaded = true;
            return;
        }

        var targetTableName = navigation.TargetEntityType.GetTableName() ?? targetClrType.Name;
        var loaded = MaterializeByIdsUntyped(store, modelBinding, entityFactory, targetClrType, targetTableName, [.. ids], takeCount: null);
        var byId = new Dictionary<int, object>(capacity: loaded.Count);
        var targetIdGetter = GetOrCompileGetter(getterCache, targetClrType, targetPk.Properties[0].Name);

        Dictionary<int, object>? trackedById = null;
        if (dbContext.ChangeTracker.QueryTrackingBehavior != QueryTrackingBehavior.NoTracking)
        {
            trackedById = new Dictionary<int, object>();
            foreach (var entry in dbContext.ChangeTracker.Entries())
            {
                var entity = entry.Entity;
                if (entity is null || entity.GetType() != targetClrType)
                    continue;

                var idObj = targetIdGetter(entity);
                if (idObj is null)
                    continue;

                trackedById[Convert.ToInt32(idObj)] = entity;
            }
        }

        for (var i = 0; i < loaded.Count; i++)
        {
            var entity = loaded[i];
            var idObj = targetIdGetter(entity);
            if (idObj is null)
                continue;

            var id = Convert.ToInt32(idObj);

            if (trackedById is not null && trackedById.TryGetValue(id, out var tracked))
            {
                byId[id] = tracked;
                continue;
            }

            TrackIfNeeded(dbContext, entity);
            knownEntities.Add(entity);
            byId[id] = entity;
        }

        var navSetter = GetOrCompileSetter(setterCache, sourceClrType, navigation.Name);

        for (var i = 0; i < sources.Count; i++)
        {
            var source = sources[i];
            var keyObj = keyGetter(source);
            if (keyObj is null)
            {
                navSetter(source, null);
                dbContext.Entry(source).Reference(navigation.Name).IsLoaded = true;
                continue;
            }

            var id = Convert.ToInt32(keyObj);
            byId.TryGetValue(id, out var target);
            navSetter(source, target);

            dbContext.Entry(source).Reference(navigation.Name).IsLoaded = true;
        }
    }

    private static void ExecuteCollectionNavigationInclude(
        DbContext dbContext,
        IMimironDb2Store store,
        MimironSQL.EntityFrameworkCore.Db2.Model.Db2ModelBinding modelBinding,
        IDb2EntityFactory entityFactory,
        IReadOnlyList<object> sources,
        INavigation navigation,
        Dictionary<(Type ClrType, string Name), Func<object, object?>> getterCache,
        Dictionary<(Type ClrType, string Name), Action<object, object?>> setterCache,
        Dictionary<Type, Func<System.Collections.IList>> listFactoryCache,
        Dictionary<(string DependentTable, int ForeignKeyFieldIndex), Dictionary<int, List<object>>> oneToManyCache,
        List<object> knownEntities)
    {
        var traceIncludes = string.Equals(Environment.GetEnvironmentVariable("MIMIRON_TRACE_INCLUDES"), "1", StringComparison.Ordinal);

        var previousAutoDetect = dbContext.ChangeTracker.AutoDetectChangesEnabled;
        dbContext.ChangeTracker.AutoDetectChangesEnabled = false;
        try
        {
        var sourceClrType = navigation.DeclaringEntityType.ClrType;
        var targetClrType = navigation.TargetEntityType.ClrType;

        var sourcePk = navigation.DeclaringEntityType.FindPrimaryKey();
        if (sourcePk is null || sourcePk.Properties.Count != 1)
            throw new NotSupportedException($"MimironDb2 collection include '{navigation.Name}' requires a single-column principal PK.");

        var principalIdGetter = GetOrCompileGetter(getterCache, sourceClrType, sourcePk.Properties[0].Name);
        var principalIds = new HashSet<int>();
        for (var i = 0; i < sources.Count; i++)
        {
            var idObj = principalIdGetter(sources[i]);
            if (idObj is null)
                continue;
            var id = Convert.ToInt32(idObj);
            if (id != 0)
                principalIds.Add(id);
        }

        var dependentTableName = navigation.TargetEntityType.GetTableName() ?? targetClrType.Name;
        var dependentSchema = store.GetSchema(dependentTableName);

        // Resolve FK field index.
        var fkProperty = navigation.ForeignKey.Properties.Single();
        var storeObject = StoreObjectIdentifier.Table(dependentTableName, schema: null);
        var fkColumnName = fkProperty.GetColumnName(storeObject) ?? fkProperty.GetColumnName() ?? fkProperty.Name;
        if (!dependentSchema.TryGetFieldCaseInsensitive(fkColumnName, out var fkField))
            throw new NotSupportedException($"MimironDb2 could not resolve FK column '{fkColumnName}' for include '{navigation.Name}'.");

        if (traceIncludes)
            Console.WriteLine($"[MimironDb2] Collection include '{navigation.DeclaringEntityType.DisplayName()}.{navigation.Name}': sources={sources.Count}, principalIds={principalIds.Count}, dependentTable={dependentTableName}, fkColumn={fkColumnName}, fkFieldIndex={fkField.ColumnStartIndex}");

        if (!oneToManyCache.TryGetValue((dependentTableName, fkField.ColumnStartIndex), out var lookup))
        {
            lookup = ScanOneToMany(store, dependentTableName, dependentSchema, modelBinding, entityFactory, principalIds, targetClrType, fkField.ColumnStartIndex, knownEntities, dbContext, getterCache, setterCache);
            oneToManyCache.Add((dependentTableName, fkField.ColumnStartIndex), lookup);

            if (traceIncludes)
                Console.WriteLine($"[MimironDb2] Collection include '{navigation.DeclaringEntityType.DisplayName()}.{navigation.Name}': scanned lookup keys={lookup.Count}");
        }

        var navSetter = GetOrCompileSetter(setterCache, sourceClrType, navigation.Name);
        var listFactory = GetOrCompileListFactory(listFactoryCache, targetClrType);

        for (var i = 0; i < sources.Count; i++)
        {
            var source = sources[i];
            var idObj = principalIdGetter(source);
            var id = idObj is null ? 0 : Convert.ToInt32(idObj);

            var list = listFactory();
            if (id != 0 && lookup.TryGetValue(id, out var dependents))
            {
                for (var j = 0; j < dependents.Count; j++)
                    list.Add(dependents[j]);
            }

            navSetter(source, list);
            dbContext.Entry(source).Collection(navigation.Name).IsLoaded = true;
        }
        }
        finally
        {
            dbContext.ChangeTracker.AutoDetectChangesEnabled = previousAutoDetect;
        }
    }

    private static Dictionary<int, List<object>> ScanOneToMany(
        IMimironDb2Store store,
        string dependentTableName,
        Db2.Schema.Db2TableSchema dependentSchema,
        MimironSQL.EntityFrameworkCore.Db2.Model.Db2ModelBinding modelBinding,
        IDb2EntityFactory entityFactory,
        HashSet<int> principalIds,
        Type dependentClrType,
        int foreignKeyFieldIndex,
        List<object> knownEntities,
        DbContext dbContext,
        Dictionary<(Type ClrType, string Name), Func<object, object?>> getterCache,
        Dictionary<(Type ClrType, string Name), Action<object, object?>> setterCache)
    {
        var lookup = new Dictionary<int, List<object>>();

        // Open once per DbContext; we still scan only once per include.
        var (file, _) = store.OpenTableWithSchema<RowHandle>(dependentTableName);

        // Materializer (typed) for the dependent entity.
        var db2EntityType = modelBinding.GetEntityType(dependentClrType).WithSchema(dependentTableName, dependentSchema);

        var materializerType = typeof(Db2EntityMaterializer<>).MakeGenericType(dependentClrType);
        var materializer = Activator.CreateInstance(materializerType, modelBinding, db2EntityType, entityFactory)
            ?? throw new InvalidOperationException($"Failed to create Db2EntityMaterializer for '{dependentClrType.FullName}'.");

        var materializeMethod = materializerType.GetMethod(nameof(Db2EntityMaterializer<object>.Materialize))
            ?? throw new InvalidOperationException("Db2EntityMaterializer.Materialize method was not found.");

        // Ensure dependent entities have non-default keys before attaching for tracking.
        // Many generated entity types follow Db2Entity<TKey>.Id conventions.
        Func<object, object?>? dependentIdGetter = null;
        Action<object, object?>? dependentIdSetter = null;
        try
        {
            dependentIdGetter = GetOrCompileGetter(getterCache, dependentClrType, "Id");
            dependentIdSetter = GetOrCompileSetter(setterCache, dependentClrType, "Id");
        }
        catch (NotSupportedException)
        {
            // If no public Id property exists, we just won't try to fix it here.
        }

        Dictionary<int, object>? trackedById = null;
        if (dbContext.ChangeTracker.QueryTrackingBehavior != QueryTrackingBehavior.NoTracking)
        {
            trackedById = new Dictionary<int, object>();
            foreach (var entry in dbContext.ChangeTracker.Entries())
            {
                var trackedEntity = entry.Entity;
                if (trackedEntity is null || trackedEntity.GetType() != dependentClrType)
                    continue;

                if (dependentIdGetter is null)
                    continue;

                var idObj = dependentIdGetter(trackedEntity);
                if (idObj is null)
                    continue;

                trackedById[Convert.ToInt32(idObj)] = trackedEntity;
            }
        }

        foreach (var handle in file.EnumerateRowHandles())
        {
            var fk = file.ReadField<int>(handle, foreignKeyFieldIndex);
            if (fk == 0)
                continue;

            if (principalIds.Count > 0 && !principalIds.Contains(fk))
                continue;

            object entity;
            var rowId = handle.RowId;

            if (trackedById is not null && trackedById.TryGetValue(rowId, out var tracked))
            {
                entity = tracked;
            }
            else
            {
                entity = (object)materializeMethod.Invoke(materializer, [file, handle])!;

                if (dependentIdGetter is not null && dependentIdSetter is not null)
                {
                    var idObj = dependentIdGetter(entity);
                    var id = idObj is null ? 0 : Convert.ToInt32(idObj);
                    if (id == 0)
                        dependentIdSetter(entity, rowId);
                }

                TrackIfNeeded(dbContext, entity);
                knownEntities.Add(entity);

                trackedById?.TryAdd(rowId, entity);
            }

            if (!lookup.TryGetValue(fk, out var list))
            {
                list = [];
                lookup.Add(fk, list);
            }

            list.Add(entity);
        }

        return lookup;
    }

    private static IReadOnlyList<object> MaterializeByIdsUntyped(
        IMimironDb2Store store,
        MimironSQL.EntityFrameworkCore.Db2.Model.Db2ModelBinding modelBinding,
        IDb2EntityFactory entityFactory,
        Type entityClrType,
        string tableName,
        IReadOnlyList<int> ids,
        int? takeCount)
    {
        var method = typeof(IMimironDb2Store)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Single(m => m.Name == nameof(IMimironDb2Store.MaterializeByIds) && m.IsGenericMethodDefinition);

        var generic = method.MakeGenericMethod(entityClrType);
        var result = generic.Invoke(store, [tableName, ids, takeCount, modelBinding, entityFactory]);
        if (result is not System.Collections.IEnumerable e)
            return [];

        var list = new List<object>();
        foreach (var item in e)
        {
            if (item is not null)
                list.Add(item);
        }

        return list;
    }

    private static Func<object, object?> GetOrCompileGetter(Dictionary<(Type ClrType, string Name), Func<object, object?>> cache, Type clrType, string name)
    {
        if (cache.TryGetValue((clrType, name), out var existing))
            return existing;

        var property = clrType.GetProperty(name, BindingFlags.Instance | BindingFlags.Public)
            ?? throw new NotSupportedException($"Property '{clrType.FullName}.{name}' was not found.");

        var obj = Expression.Parameter(typeof(object), "obj");
        var cast = Expression.Convert(obj, clrType);
        var access = Expression.Property(cast, property);
        var box = Expression.Convert(access, typeof(object));
        var lambda = Expression.Lambda<Func<object, object?>>(box, obj).Compile();

        cache[(clrType, name)] = lambda;
        return lambda;
    }

    private static Action<object, object?> GetOrCompileSetter(Dictionary<(Type ClrType, string Name), Action<object, object?>> cache, Type clrType, string name)
    {
        if (cache.TryGetValue((clrType, name), out var existing))
            return existing;

        var property = clrType.GetProperty(name, BindingFlags.Instance | BindingFlags.Public)
            ?? throw new NotSupportedException($"Property '{clrType.FullName}.{name}' was not found.");

        if (property.SetMethod is null)
            throw new NotSupportedException($"Property '{clrType.FullName}.{name}' must be writable for include fixup.");

        var obj = Expression.Parameter(typeof(object), "obj");
        var value = Expression.Parameter(typeof(object), "value");
        var castObj = Expression.Convert(obj, clrType);
        var castValue = Expression.Convert(value, property.PropertyType);
        var assign = Expression.Assign(Expression.Property(castObj, property), castValue);
        var lambda = Expression.Lambda<Action<object, object?>>(assign, obj, value).Compile();

        cache[(clrType, name)] = lambda;
        return lambda;
    }

    private static Func<System.Collections.IList> GetOrCompileListFactory(Dictionary<Type, Func<System.Collections.IList>> cache, Type elementType)
    {
        if (cache.TryGetValue(elementType, out var existing))
            return existing;

        var listType = typeof(List<>).MakeGenericType(elementType);
        var ctor = listType.GetConstructor(Type.EmptyTypes)
            ?? throw new NotSupportedException($"List<{elementType.Name}> constructor was not found.");

        var newExpr = Expression.New(ctor);
        var cast = Expression.Convert(newExpr, typeof(System.Collections.IList));
        var lambda = Expression.Lambda<Func<System.Collections.IList>>(cast).Compile();

        cache[elementType] = lambda;
        return lambda;
    }

    private static void TrackIfNeeded(DbContext dbContext, object entity)
    {
        var entry = dbContext.Entry(entity);
        if (entry.State != EntityState.Detached)
            return;

        // These entities come from a read-only store. EF's key conventions frequently mark integer keys as
        // ValueGeneratedOnAdd, and EF treats default values (e.g. 0) as temporary, which triggers generation.
        // Clear the temporary flags so we can track query results even when the key happens to be the default.
        if (entry.Metadata.FindPrimaryKey() is { } pk)
        {
            for (var i = 0; i < pk.Properties.Count; i++)
                entry.Property(pk.Properties[i].Name).IsTemporary = false;
        }

        try
        {
            entry.State = EntityState.Unchanged;
        }
        catch (NotSupportedException)
        {
            // During bootstrap, some models still use key generation conventions which reject
            // entities with default key values. Since DB2 is read-only, we can still proceed
            // with include fixup without tracking these entities.
        }
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

    private static int EvaluateLimit(QueryContext queryContext, Expression? limit)
    {
        if (limit is null)
            return -1;

        if (TryEvaluateLimitIntExpression(queryContext, limit, out var value))
            return value;

        // EF Core frequently parameterizes Take() counts (e.g., split queries / include pipelines).
        // During bootstrap we support that by reading the value from QueryContext.ParameterValues.
        if (limit.GetType().Name == "QueryParameterExpression")
        {
            const BindingFlags InstanceAnyVisibility = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

            var nameProperty = limit.GetType().GetProperty("Name", InstanceAnyVisibility);
            var nameField = limit.GetType().GetField("_name", InstanceAnyVisibility);
            var parameterName = nameProperty?.GetValue(limit) as string
                ?? nameField?.GetValue(limit) as string;

            if (string.IsNullOrWhiteSpace(parameterName))
                throw new NotSupportedException("MimironDb2 could not read QueryParameterExpression parameter name.");

            // Preferred: let EF Core evaluate the parameter expression if it provides a hook.
            // This avoids depending on QueryContext's internal parameter storage.
            var limitType = limit.GetType();
            var evalMethod = limitType
                .GetMethods(InstanceAnyVisibility)
                .FirstOrDefault(m =>
                    (m.Name is "GetValue" or "Evaluate")
                    && m.GetParameters().Length == 1
                    && m.GetParameters()[0].ParameterType.IsInstanceOfType(queryContext));

            if (evalMethod is not null)
            {
                var evaluated = evalMethod.Invoke(limit, [queryContext]);
                return evaluated switch
                {
                    int intValue => intValue,
                    null => -1,
                    _ => throw new NotSupportedException(
                        $"MimironDb2 only supports int Take() parameters during bootstrap. Parameter '{parameterName}' evaluated type: {evaluated.GetType().FullName}.")
                };
            }

            // Next best: ask QueryContext for the value. This is more robust across EF Core versions
            // than reaching into internal ParameterValues storage.
            for (var t = queryContext.GetType(); t is not null; t = t.BaseType)
            {
                var methods = t.GetMethods(InstanceAnyVisibility);

                var generic = methods.FirstOrDefault(m =>
                    m.Name == "GetParameterValue"
                    && m.IsGenericMethodDefinition
                    && m.GetGenericArguments().Length == 1
                    && m.GetParameters().Length == 1
                    && m.GetParameters()[0].ParameterType == typeof(string));

                if (generic is not null)
                {
                    var closed = generic.MakeGenericMethod(typeof(int));
                    var evaluated = closed.Invoke(queryContext, [parameterName]);
                    return evaluated switch
                    {
                        int intValue => intValue,
                        null => -1,
                        _ => throw new NotSupportedException(
                            $"MimironDb2 only supports int Take() parameters during bootstrap. QueryContext.GetParameterValue<int>('{parameterName}') returned type: {evaluated.GetType().FullName}.")
                    };
                }

                var nongeneric = methods.FirstOrDefault(m =>
                    m.Name == "GetParameterValue"
                    && !m.IsGenericMethodDefinition
                    && m.GetParameters().Length == 1
                    && m.GetParameters()[0].ParameterType == typeof(string));

                if (nongeneric is not null)
                {
                    var evaluated = nongeneric.Invoke(queryContext, [parameterName]);
                    return evaluated switch
                    {
                        int intValue => intValue,
                        null => -1,
                        _ => throw new NotSupportedException(
                            $"MimironDb2 only supports int Take() parameters during bootstrap. QueryContext.GetParameterValue('{parameterName}') returned type: {evaluated.GetType().FullName}.")
                    };
                }
            }

            var queryContextType = queryContext.GetType();
            object? parameterValuesObject = null;
            for (var t = queryContextType; t is not null && parameterValuesObject is null; t = t.BaseType)
            {
                var parameterValuesProperty = t.GetProperty("ParameterValues", InstanceAnyVisibility);
                var parameterValuesField = t.GetField("_parameterValues", InstanceAnyVisibility)
                    ?? t.GetField("ParameterValues", InstanceAnyVisibility);

                parameterValuesObject = parameterValuesProperty?.GetValue(queryContext)
                    ?? parameterValuesField?.GetValue(queryContext);
            }

            // If we can't access the internal parameter values, we cannot safely evaluate the limit.
            // Failing open here breaks terminal operator semantics (e.g., Any() rewritten to SingleOrDefault).
            if (parameterValuesObject is null)
                throw new NotSupportedException(
                    $"MimironDb2 could not access QueryContext parameter values to evaluate Take() parameter '{parameterName}'.");

            IReadOnlyDictionary<string, object?> parameterValues;
            if (parameterValuesObject is IReadOnlyDictionary<string, object?> ro)
            {
                parameterValues = ro;
            }
            else if (parameterValuesObject is IDictionary<string, object?> rw)
            {
                parameterValues = new Dictionary<string, object?>(rw);
            }
            else if (parameterValuesObject is System.Collections.IDictionary nongeneric)
            {
                var copy = new Dictionary<string, object?>();
                foreach (var key in nongeneric.Keys)
                {
                    if (key is string s)
                        copy[s] = nongeneric[key];
                }

                parameterValues = copy;
            }
            else
            {
                throw new NotSupportedException(
                    $"MimironDb2 QueryContext.ParameterValues has unexpected type '{parameterValuesObject.GetType().FullName}'.");
            }

            if (!parameterValues.TryGetValue(parameterName, out var boxedValue))
                throw new NotSupportedException(
                    $"MimironDb2 could not find Take() parameter '{parameterName}' in QueryContext parameter values.");

            return boxedValue switch
            {
                int intValue => intValue,
                null => -1,
                _ => throw new NotSupportedException(
                    $"MimironDb2 only supports int Take() parameters during bootstrap. Parameter '{parameterName}' type: {boxedValue?.GetType().FullName ?? "<null>"}.")
            };
        }

        throw new NotSupportedException(
            $"MimironDb2 only supports Take() limits which can be safely evaluated during bootstrap. Limit expression type: {limit.GetType().Name}. Expression: {limit}.");
    }

    private static bool TryEvaluateLimitIntExpression(QueryContext queryContext, Expression expression, out int value)
    {
        // Bootstrap policy:
        // - Allow constants and EF Core query parameters.
        // - Allow Math.Min/Math.Max for multiple Take() calls.
        // - Allow captured integers via closure fields (common for controller parameters).
        // - Do not compile/invoke arbitrary expression trees.

        const int MaxDepth = 32;
        return TryEvaluateLimitIntExpression(queryContext, expression, depth: 0, out value);

        static bool TryEvaluateLimitIntExpression(QueryContext queryContext, Expression expression, int depth, out int value)
        {
            if (depth > MaxDepth)
            {
                value = default;
                return false;
            }

            // Strip conversions.
            while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                expression = u.Operand;

            if (expression is ConstantExpression { Value: int i })
            {
                value = i;
                return true;
            }

            // Captured variable: closure field access.
            if (expression is MemberExpression { Member: FieldInfo field, Expression: ConstantExpression { Value: { } closure } })
            {
                var fieldValue = field.GetValue(closure);
                if (fieldValue is int fi)
                {
                    value = fi;
                    return true;
                }
            }

            // Multiple Take() calls are represented as Math.Min(oldLimit, newLimit).
            if (expression is MethodCallExpression call
                && call.Method.DeclaringType == typeof(Math)
                && call.Arguments.Count == 2
                && (call.Method.Name == nameof(Math.Min) || call.Method.Name == nameof(Math.Max))
                && call.Method.ReturnType == typeof(int))
            {
                if (!TryEvaluateLimitIntExpression(queryContext, call.Arguments[0], depth + 1, out var left))
                {
                    value = default;
                    return false;
                }

                if (!TryEvaluateLimitIntExpression(queryContext, call.Arguments[1], depth + 1, out var right))
                {
                    value = default;
                    return false;
                }

                value = call.Method.Name == nameof(Math.Min)
                    ? Math.Min(left, right)
                    : Math.Max(left, right);

                return true;
            }

            // EF Core frequently parameterizes Take() counts (e.g., split queries / include pipelines).
            // During bootstrap we support that by reading the value from QueryContext.ParameterValues.
            if (expression.GetType().Name == "QueryParameterExpression")
            {
                value = EvaluateQueryParameterInt(queryContext, expression);
                return true;
            }

            value = default;
            return false;
        }

        static int EvaluateQueryParameterInt(QueryContext queryContext, Expression limit)
        {
            const BindingFlags InstanceAnyVisibility = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

            var nameProperty = limit.GetType().GetProperty("Name", InstanceAnyVisibility);
            var nameField = limit.GetType().GetField("_name", InstanceAnyVisibility);
            var parameterName = nameProperty?.GetValue(limit) as string
                ?? nameField?.GetValue(limit) as string;

            if (string.IsNullOrWhiteSpace(parameterName))
                throw new NotSupportedException("MimironDb2 could not read QueryParameterExpression parameter name.");

            // Preferred: let EF Core evaluate the parameter expression if it provides a hook.
            // This avoids depending on QueryContext's internal parameter storage.
            var limitType = limit.GetType();
            var evalMethod = limitType
                .GetMethods(InstanceAnyVisibility)
                .FirstOrDefault(m =>
                    (m.Name is "GetValue" or "Evaluate")
                    && m.GetParameters().Length == 1
                    && m.GetParameters()[0].ParameterType.IsInstanceOfType(queryContext));

            if (evalMethod is not null)
            {
                var evaluated = evalMethod.Invoke(limit, [queryContext]);
                return evaluated switch
                {
                    int value => value,
                    null => -1,
                    _ => throw new NotSupportedException(
                        $"MimironDb2 only supports int Take() parameters during bootstrap. Parameter '{parameterName}' evaluated type: {evaluated.GetType().FullName}.")
                };
            }

            var queryContextType = queryContext.GetType();
            object? parameterValuesObject = null;
            for (var t = queryContextType; t is not null && parameterValuesObject is null; t = t.BaseType)
            {
                var parameterValuesProperty = t.GetProperty("ParameterValues", InstanceAnyVisibility);
                var parameterValuesField = t.GetField("_parameterValues", InstanceAnyVisibility)
                    ?? t.GetField("ParameterValues", InstanceAnyVisibility);

                parameterValuesObject = parameterValuesProperty?.GetValue(queryContext)
                    ?? parameterValuesField?.GetValue(queryContext);
            }

            // If we can't access the internal parameter values, fall back to "no limit".
            // This keeps bootstrap execution working; correctness for parameterized Take() will be improved later.
            if (parameterValuesObject is null)
                return -1;

            IReadOnlyDictionary<string, object?> parameterValues;
            if (parameterValuesObject is IReadOnlyDictionary<string, object?> ro)
            {
                parameterValues = ro;
            }
            else if (parameterValuesObject is IDictionary<string, object?> rw)
            {
                parameterValues = new Dictionary<string, object?>(rw);
            }
            else if (parameterValuesObject is System.Collections.IDictionary nongeneric)
            {
                var copy = new Dictionary<string, object?>();
                foreach (var key in nongeneric.Keys)
                {
                    if (key is string s)
                        copy[s] = nongeneric[key];
                }

                parameterValues = copy;
            }
            else
            {
                throw new NotSupportedException(
                    $"MimironDb2 QueryContext.ParameterValues has unexpected type '{parameterValuesObject.GetType().FullName}'.");
            }

            if (!parameterValues.TryGetValue(parameterName, out var boxedValue))
                return -1;

            return boxedValue switch
            {
                int value => value,
                null => -1,
                _ => throw new NotSupportedException(
                    $"MimironDb2 only supports int Take() parameters during bootstrap. Parameter '{parameterName}' type: {boxedValue?.GetType().FullName ?? "<null>"}.")
            };
        }
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
        if (TryGetTransparentIdentifierTypes(expectedParameterType, out var outerType, out var innerType))
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

        // Wrap into Func<QueryContext, object, bool> without DynamicInvoke.
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

    private static bool TryGetTransparentIdentifierTypes(Type type, out Type outer, out Type inner)
    {
        if (type.IsGenericType
            && type.DeclaringType is not null
            && type.DeclaringType.Name == "TransparentIdentifierFactory"
            && type.Name.StartsWith("TransparentIdentifier`", StringComparison.Ordinal)
            && type.GetGenericArguments() is { Length: 2 } args)
        {
            outer = args[0];
            inner = args[1];
            return true;
        }

        outer = null!;
        inner = null!;
        return false;
    }

    private static IEnumerable<ValueBuffer> Table(QueryContext queryContext, Db2QueryExpression queryExpression)
    {
        var dbContext = queryContext.Context;

        var entityType = queryExpression.EntityType;
        var tableName = entityType.GetTableName() ?? entityType.ClrType.Name;

        var options = dbContext.GetService<IDbContextOptions>();
        var extension = options.FindExtension<MimironDb2OptionsExtension>();

        var wowVersion = extension?.WowVersion;
        if (string.IsNullOrWhiteSpace(wowVersion))
            throw new InvalidOperationException("MimironDb2 WOW_VERSION is not configured. Call UseMimironDb2(o => o.WithWowVersion(...)).");

        var store = dbContext.GetService<IMimironDb2Store>();
        // Prefer DI-provided format, but default to WDC5 for now.
        var format = dbContext.GetService<IDb2Format>() ?? new Wdc5Format();

        if (queryExpression.Joins.Count == 0)
        {
            var (typedFile, schema) = store.OpenTableWithSchema<RowHandle>(tableName);
            var layout = format.GetLayout(typedFile);

            var propertiesToRead = GetPropertiesToRead(entityType, additionalRequired: []);
            var valueBufferLength = GetEntityValueBufferLength(entityType);
            var readPlan = ValueBufferReadPlanCache.GetOrCreate(entityType, tableName, wowVersion, layout.LayoutHash, schema, valueBufferLength, propertiesToRead);

            foreach (var handle in typedFile.EnumerateRowHandles())
            {
                var values = valueBufferLength > 0 ? new object?[valueBufferLength] : Array.Empty<object?>();

                foreach (var entry in readPlan.Entries)
                    values[entry.PropertyIndex] = entry.Reader(typedFile, handle);

                yield return new ValueBuffer(values);
            }

            yield break;
        }

        // Joins are represented as a left-deep tree on the query root.
        // Support Join/LeftJoin for Include/ThenInclude execution during bootstrap.
        var joins = queryExpression.Joins;

        var rootSchemaForJoin = store.GetSchema(tableName);

        var rootValueBufferLength = GetEntityValueBufferLength(entityType);
        var rootPropertiesToRead = GetPropertiesToRead(entityType, additionalRequired: []);

        var joinPlans = new List<JoinPlan>(joins.Count);

        // Slot 0 is the root entity, subsequent slots are join inners in order.
        var slotEntityTypes = new List<IEntityType>(capacity: joins.Count + 1) { entityType };
        var slotOffsets = new List<int>(capacity: joins.Count + 1) { 0 };
        var runningOffset = rootValueBufferLength;

        // Cache lookup materialization per (table,keyIndex) to avoid rebuilding for repeated includes.
        var lookupCache = new Dictionary<(string TableName, int KeyIndex), Dictionary<object, List<object?[]>>>(new LookupCacheKeyComparer());

        for (var joinIndex = 0; joinIndex < joins.Count; joinIndex++)
        {
            var (joinOperator, innerQuery, outerKeySelector, innerKeySelector) = joins[joinIndex];

            if (joinOperator is not (nameof(Queryable.Join) or nameof(Queryable.LeftJoin)))
                throw new NotSupportedException($"MimironDb2 join execution currently only supports Queryable.Join and Queryable.LeftJoin. Saw '{joinOperator}'.");

            if (innerQuery.Joins.Count != 0)
                throw new NotSupportedException("MimironDb2 join execution does not currently support nested joins on the inner query.");

            var innerEntityType = innerQuery.EntityType;
            var innerTableName = innerEntityType.GetTableName() ?? innerEntityType.ClrType.Name;

            var innerSchemaForJoin = store.GetSchema(innerTableName);
            var innerValueBufferLength = GetEntityValueBufferLength(innerEntityType);

            // Resolve key references.
            var outerKeyRef = ResolveJoinKeyReference(slotEntityTypes, outerKeySelector);
            var innerKeyProperty = GetJoinKeyProperty(innerEntityType, innerKeySelector);
            var innerKeyIndex = innerKeyProperty.GetIndex();

            var innerPropertiesToRead = GetPropertiesToRead(innerEntityType, additionalRequired: [innerKeyProperty]);

            // Build inner lookup.
            if (!lookupCache.TryGetValue((innerTableName, innerKeyIndex), out var innerLookup))
            {
                innerLookup = BuildLookup(
                    store,
                    format,
                    innerTableName,
                    innerSchemaForJoin,
                    wowVersion,
                    innerEntityType,
                    innerValueBufferLength,
                    innerPropertiesToRead,
                    innerKeyProperty);

                lookupCache.Add((innerTableName, innerKeyIndex), innerLookup);
            }

            joinPlans.Add(new JoinPlan(
                joinOperator,
                innerEntityType,
                innerTableName,
                innerValueBufferLength,
                innerPropertiesToRead,
                outerKeyRef,
                innerKeyIndex,
                innerLookup,
                runningOffset));

            slotEntityTypes.Add(innerEntityType);
            slotOffsets.Add(runningOffset);
            runningOffset += innerValueBufferLength;
        }

        var (rootFile, _) = store.OpenTableWithSchema<RowHandle>(tableName);
        var rootLayoutForJoin = format.GetLayout(rootFile);

        var rootReadPlan = ValueBufferReadPlanCache.GetOrCreate(entityType, tableName, wowVersion, rootLayoutForJoin.LayoutHash, rootSchemaForJoin, rootValueBufferLength, rootPropertiesToRead);
        foreach (var rootHandle in rootFile.EnumerateRowHandles())
        {
            var rootValues = rootValueBufferLength > 0 ? new object?[rootValueBufferLength] : Array.Empty<object?>();
            foreach (var entry in rootReadPlan.Entries)
                rootValues[entry.PropertyIndex] = entry.Reader(rootFile, rootHandle);

            // Apply joins in order, expanding rows for one-to-many.
            var currentRows = new List<object?[]>(capacity: 1) { rootValues };

            foreach (var plan in joinPlans)
            {
                var nextRows = new List<object?[]>();

                foreach (var row in currentRows)
                {
                    var outerKey = GetValue(row, plan.OuterKeyRef);

                    if (outerKey is null)
                    {
                        if (plan.JoinOperator == nameof(Queryable.LeftJoin))
                        {
                            nextRows.Add(Concat(row, plan.Offset, plan.InnerValueBufferLength, null));
                        }

                        continue;
                    }

                    if (!plan.InnerLookup.TryGetValue(outerKey, out var innerMatches) || innerMatches.Count == 0)
                    {
                        if (plan.JoinOperator == nameof(Queryable.LeftJoin))
                        {
                            nextRows.Add(Concat(row, plan.Offset, plan.InnerValueBufferLength, null));
                        }

                        continue;
                    }

                    foreach (var inner in innerMatches)
                    {
                        nextRows.Add(Concat(row, plan.Offset, plan.InnerValueBufferLength, inner));
                    }
                }

                currentRows = nextRows;
                if (currentRows.Count == 0)
                    break;
            }

            foreach (var combined in currentRows)
            {
                var final = combined;
                if (combined.Length != runningOffset)
                {
                    // Ensure the ValueBuffer matches the joined layout even when there are no joins.
                    final = combined;
                }

                yield return new ValueBuffer(final);
            }
        }
    }

    private readonly record struct JoinKeyReference(int SlotIndex, int PropertyIndex, int SlotOffset)
    {
        public int AbsoluteIndex => SlotOffset + PropertyIndex;
    }

    private sealed record JoinPlan(
        string JoinOperator,
        IEntityType InnerEntityType,
        string InnerTableName,
        int InnerValueBufferLength,
        IProperty[] InnerPropertiesToRead,
        JoinKeyReference OuterKeyRef,
        int InnerKeyIndex,
        Dictionary<object, List<object?[]>> InnerLookup,
        int Offset);

    private static int GetEntityValueBufferLength(IEntityType entityType)
        => !entityType.GetProperties().Any() ? 0 : entityType.GetProperties().Max(static p => p.GetIndex()) + 1;

    private static IProperty[] GetPropertiesToRead(IEntityType entityType, IReadOnlyCollection<IProperty> additionalRequired)
    {
        var set = new HashSet<IProperty>();

        foreach (var p in entityType.GetProperties())
        {
            if (p.IsShadowProperty())
                continue;

            if (p.PropertyInfo is null)
                continue;

            set.Add(p);
        }

        foreach (var p in additionalRequired)
            set.Add(p);

        return set.OrderBy(static p => p.GetIndex()).ToArray();
    }

    private static Dictionary<object, List<object?[]>> BuildLookup(
        IMimironDb2Store store,
        IDb2Format format,
        string tableName,
        Db2.Schema.Db2TableSchema schema,
        string wowVersion,
        IEntityType entityType,
        int valueBufferLength,
        IProperty[] propertiesToRead,
        IProperty keyProperty)
    {
        var keyIndex = keyProperty.GetIndex();
        var lookup = new Dictionary<object, List<object?[]>>();

        var (file, _) = store.OpenTableWithSchema<RowHandle>(tableName);
        var layout = format.GetLayout(file);

        var readPlan = ValueBufferReadPlanCache.GetOrCreate(entityType, tableName, wowVersion, layout.LayoutHash, schema, valueBufferLength, propertiesToRead);
        foreach (var handle in file.EnumerateRowHandles())
        {
            var values = valueBufferLength > 0 ? new object?[valueBufferLength] : Array.Empty<object?>();

            foreach (var entry in readPlan.Entries)
                values[entry.PropertyIndex] = entry.Reader(file, handle);

            var key = values.Length > keyIndex ? values[keyIndex] : null;
            if (key is null)
                continue;

            if (!lookup.TryGetValue(key, out var list))
            {
                list = [];
                lookup.Add(key, list);
            }

            list.Add(values);
        }

        return lookup;
    }

    private readonly record struct ValueBufferReadPlanEntry(int PropertyIndex, Func<IDb2File, RowHandle, object?> Reader);

    private sealed class ValueBufferReadPlan
    {
        public required int ValueBufferLength { get; init; }
        public required ValueBufferReadPlanEntry[] Entries { get; init; }
    }

    private sealed class ValueBufferReadPlanCache
    {
        private readonly record struct CacheKey(
            IEntityType EntityType,
            string TableName,
            string WowVersion,
            uint LayoutHash,
            string PropertySetKey);

        private static readonly ConcurrentDictionary<CacheKey, ValueBufferReadPlan> Cache = new();

        private readonly record struct ReaderCacheKey(Type ReadType, Type ResultType, int FieldIndex);

        private static readonly ConcurrentDictionary<ReaderCacheKey, Func<IDb2File, RowHandle, object?>> ReaderCache = new();

        public static ValueBufferReadPlan GetOrCreate(
            IEntityType entityType,
            string tableName,
            string wowVersion,
            uint layoutHash,
            Db2.Schema.Db2TableSchema schema,
            int valueBufferLength,
            IReadOnlyList<IProperty> propertiesToRead)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ArgumentNullException.ThrowIfNull(tableName);
            ArgumentNullException.ThrowIfNull(wowVersion);
            ArgumentNullException.ThrowIfNull(schema);
            ArgumentNullException.ThrowIfNull(propertiesToRead);

            var ordered = propertiesToRead.Count == 0
                ? Array.Empty<IProperty>()
                : propertiesToRead.OrderBy(static p => p.GetIndex()).ToArray();

            var propertySetKey = CreatePropertySetKey(ordered);
            var key = new CacheKey(entityType, tableName, wowVersion, layoutHash, propertySetKey);

            return Cache.GetOrAdd(key, _ => Build(entityType, tableName, schema, valueBufferLength, ordered));
        }

        private static ValueBufferReadPlan Build(
            IEntityType entityType,
            string tableName,
            Db2.Schema.Db2TableSchema schema,
            int valueBufferLength,
            IReadOnlyList<IProperty> orderedPropertiesToRead)
        {
            var storeObject = StoreObjectIdentifier.Table(tableName, schema: null);
            var entries = new List<ValueBufferReadPlanEntry>(capacity: orderedPropertiesToRead.Count);

            foreach (var property in orderedPropertiesToRead)
            {
                var columnName = property.GetColumnName(storeObject) ?? property.GetColumnName() ?? property.Name;
                if (!schema.TryGetFieldCaseInsensitive(columnName, out var fieldSchema))
                    continue;

                var resultType = property.ClrType;
                var readType = GetReadTypeForReadField(resultType);
                var reader = ReaderCache.GetOrAdd(new ReaderCacheKey(readType, resultType, fieldSchema.ColumnStartIndex), static key => CompileReader(key.ReadType, key.ResultType, key.FieldIndex));
                entries.Add(new ValueBufferReadPlanEntry(property.GetIndex(), reader));
            }

            return new ValueBufferReadPlan
            {
                ValueBufferLength = valueBufferLength,
                Entries = entries.ToArray(),
            };
        }

        private static Type GetReadTypeForReadField(Type resultType)
        {
            ArgumentNullException.ThrowIfNull(resultType);

            if (resultType.IsArray)
                return resultType;

            // For nullable properties, ReadField<T> expects the underlying non-nullable type.
            var unwrapped = Nullable.GetUnderlyingType(resultType);
            return unwrapped ?? resultType;
        }

        private static Func<IDb2File, RowHandle, object?> CompileReader(Type readType, Type resultType, int fieldIndex)
        {
            var file = Expression.Parameter(typeof(IDb2File), "file");
            var handle = Expression.Parameter(typeof(RowHandle), "handle");

            var readMethod = typeof(IDb2File)
                .GetMethod(nameof(IDb2File.ReadField), BindingFlags.Instance | BindingFlags.Public)!
                .MakeGenericMethod(readType);

            Expression readCall = Expression.Call(file, readMethod, handle, Expression.Constant(fieldIndex));
            if (readType != resultType)
                readCall = Expression.Convert(readCall, resultType);

            var boxed = Expression.Convert(readCall, typeof(object));
            return Expression.Lambda<Func<IDb2File, RowHandle, object?>>(boxed, file, handle).Compile();
        }

        private static string CreatePropertySetKey(IReadOnlyList<IProperty> orderedProperties)
        {
            if (orderedProperties.Count == 0)
                return string.Empty;

            var sb = new StringBuilder(capacity: orderedProperties.Count * 4);
            for (var i = 0; i < orderedProperties.Count; i++)
            {
                if (i != 0)
                    sb.Append(',');
                sb.Append(orderedProperties[i].GetIndex());
            }

            return sb.ToString();
        }
    }

    private static JoinKeyReference ResolveJoinKeyReference(IReadOnlyList<IEntityType> slotEntityTypes, LambdaExpression outerKeySelector)
    {
        ArgumentNullException.ThrowIfNull(slotEntityTypes);
        ArgumentNullException.ThrowIfNull(outerKeySelector);

        if (outerKeySelector.Parameters.Count != 1)
            throw new NotSupportedException("MimironDb2 join key selectors must have exactly one parameter.");

        var parameter = outerKeySelector.Parameters[0];
        var body = outerKeySelector.Body;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            body = u.Operand;

        string propertyName;
        Expression instance;

        if (body is MethodCallExpression
            {
                Method.Name: nameof(EF.Property),
                Arguments: [var inst, ConstantExpression { Value: string s }]
            })
        {
            instance = inst;
            propertyName = s;
        }
        else if (body is MemberExpression { Member: PropertyInfo p, Expression: { } instExpr })
        {
            instance = instExpr;
            propertyName = p.Name;
        }
        else
        {
            throw new NotSupportedException($"MimironDb2 join execution only supports simple property key selectors. Saw '{outerKeySelector.Body}'.");
        }

        var slotIndex = ResolveSlotIndexFromInstance(parameter, instance);
        if ((uint)slotIndex >= (uint)slotEntityTypes.Count)
            throw new NotSupportedException($"MimironDb2 could not resolve join key slot index '{slotIndex}' (slots={slotEntityTypes.Count}).");

        var entityType = slotEntityTypes[slotIndex];
        var property = entityType.FindProperty(propertyName)
            ?? throw new NotSupportedException($"MimironDb2 could not resolve join key property '{propertyName}' on '{entityType.DisplayName()}'.");

        // Slot offsets are a fixed span layout: sum of entity value buffer lengths for prior slots.
        var slotOffset = 0;
        for (var i = 0; i < slotIndex; i++)
            slotOffset += GetEntityValueBufferLength(slotEntityTypes[i]);

        return new JoinKeyReference(slotIndex, property.GetIndex(), slotOffset);
    }

    private static int ResolveSlotIndexFromInstance(ParameterExpression parameter, Expression instance)
    {
        // instance is expected to be either the parameter itself (root entity) or a chain of .Outer/.Inner
        // member accesses over EF Core's nested TransparentIdentifier type.
        if (instance == parameter)
        {
            if (TryGetTransparentIdentifierTypes(parameter.Type, out _, out _))
                throw new NotSupportedException("MimironDb2 cannot use the entire TransparentIdentifier as a join key instance.");

            return 0;
        }

        var steps = new List<string>();
        var current = instance;
        while (current != parameter)
        {
            if (current is not MemberExpression { Expression: { } parent, Member.Name: var name }
                || (name != "Outer" && name != "Inner"))
            {
                throw new NotSupportedException($"MimironDb2 cannot resolve join key instance '{instance}'.");
            }

            steps.Add(name);
            current = parent;
        }

        steps.Reverse();

        var type = parameter.Type;
        if (!TryGetTransparentIdentifierTypes(type, out _, out _))
            throw new NotSupportedException("MimironDb2 expected a TransparentIdentifier parameter for this join key selector.");

        var baseIndex = 0;
        foreach (var step in steps)
        {
            if (!TryGetTransparentIdentifierTypes(type, out var outerType, out var innerType))
                throw new NotSupportedException($"MimironDb2 cannot traverse '{step}' on non-TransparentIdentifier type '{type.FullName}'.");

            if (step == "Outer")
            {
                type = outerType;
                continue;
            }

            // Inner: select the leaf index of the current inner.
            var outerLeaves = CountTransparentIdentifierLeaves(outerType);
            return baseIndex + outerLeaves;
        }

        // Ending on the outer-most leaf.
        return baseIndex;
    }

    private static int CountTransparentIdentifierLeaves(Type type)
        => TryGetTransparentIdentifierTypes(type, out var outer, out _) ? CountTransparentIdentifierLeaves(outer) + 1 : 1;

    private static object? GetValue(object?[] row, JoinKeyReference keyRef)
    {
        var index = keyRef.AbsoluteIndex;
        return (uint)index < (uint)row.Length ? row[index] : null;
    }

    private static object?[] Concat(object?[] outer, int offset, int innerLength, object?[]? inner)
    {
        var total = offset + innerLength;
        if (outer.Length != offset)
        {
            // Outer may already include previous joins; offset is the current outer span length.
            total = outer.Length + innerLength;
            offset = outer.Length;
        }

        var combined = total > 0 ? new object?[total] : Array.Empty<object?>();
        if (outer.Length > 0)
            Array.Copy(outer, 0, combined, 0, outer.Length);

        if (inner is not null && innerLength > 0)
            Array.Copy(inner, 0, combined, offset, Math.Min(innerLength, inner.Length));

        // If inner is null (LeftJoin no match), keep the inner segment as all-null.
        return combined;
    }

    private sealed class LookupCacheKeyComparer : IEqualityComparer<(string TableName, int KeyIndex)>
    {
        public bool Equals((string TableName, int KeyIndex) x, (string TableName, int KeyIndex) y)
            => StringComparer.OrdinalIgnoreCase.Equals(x.TableName, y.TableName) && x.KeyIndex == y.KeyIndex;

        public int GetHashCode((string TableName, int KeyIndex) obj)
            => HashCode.Combine(StringComparer.OrdinalIgnoreCase.GetHashCode(obj.TableName), obj.KeyIndex);
    }

    private static IProperty GetJoinKeyProperty(IEntityType entityType, LambdaExpression keySelector)
    {
        ArgumentNullException.ThrowIfNull(entityType);
        ArgumentNullException.ThrowIfNull(keySelector);

        var body = keySelector.Body;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            body = u.Operand;

        if (body is MethodCallExpression
            {
                Method.Name: nameof(EF.Property),
                Arguments: [_, ConstantExpression { Value: string propertyName }]
            })
        {
            return entityType.FindProperty(propertyName)
                ?? throw new NotSupportedException($"MimironDb2 could not resolve join key property '{propertyName}' on '{entityType.DisplayName()}'.");
        }

        if (body is not MemberExpression { Member: PropertyInfo prop })
            throw new NotSupportedException($"MimironDb2 join execution only supports simple property key selectors. Saw '{keySelector.Body}'.");

        return entityType.FindProperty(prop.Name)
            ?? throw new NotSupportedException($"MimironDb2 could not resolve join key property '{prop.Name}' on '{entityType.DisplayName()}'.");
    }
}