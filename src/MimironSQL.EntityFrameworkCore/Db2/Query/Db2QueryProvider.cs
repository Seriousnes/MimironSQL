using MimironSQL.Formats;

using System.Collections.Concurrent;
using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Query;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

internal sealed class Db2QueryProvider<TEntity, TRow>(
    IDb2File<TRow> file,
    Db2ModelBinding model,
    Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
    IDb2EntityFactory entityFactory) : IQueryProvider
    where TEntity : class
    where TRow : struct, IRowHandle
{
    private static readonly ConcurrentDictionary<Type, Func<Db2QueryProvider<TEntity, TRow>, Expression, IEnumerable>> ExecuteEnumerableDelegates = new();

    private readonly Db2EntityType _rootEntityType = model.GetEntityType(typeof(TEntity));
    private readonly IDb2EntityFactory _entityFactory = entityFactory ?? throw new ArgumentNullException(nameof(entityFactory));
    private readonly Db2EntityMaterializer<TEntity> _materializer = new(model, model.GetEntityType(typeof(TEntity)), entityFactory);
    private readonly Db2ModelBinding _model = model;
    private readonly Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> _tableResolver = tableResolver;

    public IQueryable CreateQuery(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        var elementType = expression.Type.GetGenericArguments().FirstOrDefault() ?? typeof(string).BaseType!;

        // Non-generic CreateQuery is not used by the normal strongly-typed Queryable surface.
        // Keep it functional without using reflection invocation.
        var factoryMethod = typeof(Db2QueryProvider<TEntity, TRow>)
            .GetMethod(nameof(CreateQueryableFactory), BindingFlags.Static | BindingFlags.NonPublic)!;

        var generic = factoryMethod.MakeGenericMethod(elementType);
        var getFactory = generic.CreateDelegate<Func<Func<IQueryProvider, Expression, IQueryable>>>();
        var factory = getFactory();
        return factory(this, expression);
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        return new Db2Queryable<TElement>(this, expression);
    }

    public object? Execute(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        // Non-generic Execute returns a boxed scalar for scalar results.
        // This provider expects callers to use Execute<TResult>.
        if (!TryGetEnumerableElementType(expression.Type, out var elementType))
            throw new NotSupportedException("Use Execute<TResult>(...) instead of the non-generic Execute(...) for this provider.");

        return GetExecuteEnumerableDelegate(elementType)(this, expression);
    }

    public TResult Execute<TResult>(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        if (TryGetEnumerableElementType(typeof(TResult), out var elementType))
        {
            if (typeof(TResult).IsGenericType && typeof(TResult).GetGenericTypeDefinition() == typeof(IQueryable<>))
                throw new NotSupportedException("Execute<TResult> does not support IQueryable results; use CreateQuery instead.");

            return (TResult)GetExecuteEnumerableDelegate(elementType)(this, expression);
        }

        return ExecuteScalar<TResult>(expression);
    }

    private static Func<IQueryProvider, Expression, IQueryable> CreateQueryableFactory<TElement>()
        => static (provider, expression) => new Db2Queryable<TElement>(provider, expression);

    private static IEnumerable ExecuteEnumerableForResult<TElement>(Db2QueryProvider<TEntity, TRow> provider, Expression expression)
        => provider.ExecuteEnumerable<TElement>(expression);

    private static Func<Db2QueryProvider<TEntity, TRow>, Expression, IEnumerable> GetExecuteEnumerableDelegate(Type elementType)
        => ExecuteEnumerableDelegates.GetOrAdd(elementType, static elementType =>
        {
            var method = typeof(Db2QueryProvider<TEntity, TRow>).GetMethod(
                nameof(ExecuteEnumerableForResult),
                BindingFlags.Static | BindingFlags.NonPublic)!
                .MakeGenericMethod(elementType);

            return method.CreateDelegate<Func<Db2QueryProvider<TEntity, TRow>, Expression, IEnumerable>>();
        });

    // ──────── Core query execution ────────

    private IEnumerable<TElement> ExecuteEnumerable<TElement>(Expression expression)
    {
        // Step 1: Preprocess — strip Include / EF modifiers, collect include chains.
        var preprocessed = Db2ExpressionPreprocessor.Preprocess(expression);
        var ignoreAutoIncludes = preprocessed.IgnoreAutoIncludes;

        // Step 2: Walk the cleaned expression to extract pre-entity information.
        var analysis = AnalyzePreEntityChain(preprocessed.CleanedExpression);

        // Step 3: Expand include chains (auto-includes) and build root member set.
        var includeChains = preprocessed.IncludeChains.Select(static c => c.ToArray()).ToList();
        if (!ignoreAutoIncludes)
            includeChains = ExpandIncludesWithAutoIncludes(includeChains);

        // Reject Include after a Select that changes the element type.
        for (var i = 0; i < includeChains.Count; i++)
        {
            if (includeChains[i].Length > 0 && includeChains[i][0].DeclaringType != typeof(TEntity))
            {
                throw new NotSupportedException(
                    $"Include after a Select that changes the element type is not supported. " +
                    $"Move the Include before the Select, or remove the type-changing projection.");
            }
        }

        var includedRootMembers = includeChains
            .Where(static chain => chain.Length != 0)
            .Select(static chain => chain[0])
            .ToHashSet();

        // Step 4: Enforce the explicit Include requirement for any root navigation usage.
        for (var i = 0; i < analysis.PreEntityWherePredicates.Count; i++)
            Db2IncludePolicy.ThrowIfNavigationRequiresInclude(_model, includedRootMembers, analysis.PreEntityWherePredicates[i]);

        // Validate Select doesn't use navigations without Include.
        if (analysis.SelectSelector is not null)
            Db2IncludePolicy.ThrowIfNavigationRequiresInclude(_model, includedRootMembers, analysis.SelectSelector);

        // Validate post-entity lambdas.
        foreach (var lambda in analysis.PostEntityLambdas)
            Db2IncludePolicy.ThrowIfNavigationRequiresInclude(_model, includedRootMembers, lambda);

        // Step 5: Try pruned execution path (row-level projectors, bypass entity materialization).
        if (analysis.SelectSelector is not null && includeChains.Count == 0)
        {
            var canAttemptPrune =
                analysis.SelectSelector is { Parameters.Count: 1 } &&
                analysis.SelectSelector.Parameters[0].Type == typeof(TEntity) &&
                analysis.SelectSelector.ReturnType == typeof(TElement) &&
                !Db2IncludePolicy.UsesRootNavigation(_model, analysis.SelectSelector) &&
                !analysis.HasPostSelectOperations &&
                analysis.PostEntityLambdas.Count == 0;

            if (canAttemptPrune)
            {
                if (TryExecuteEnumerablePruned<TElement>(analysis, out var pruned))
                    return pruned;
            }
        }

        // Step 6: Enumerate entities with row-level optimizations.
        var entities = EnumerateEntities(analysis.PreEntityWherePredicates, analysis.PreSkipCount, analysis.PreTakeCount).ToList();

        // Step 7: Apply includes.
        IEnumerable<TEntity> current = entities;
        for (var i = 0; i < includeChains.Count; i++)
            current = Db2IncludeChainExecutor.Apply(current, _model, _tableResolver, includeChains[i], _entityFactory);

        // Step 8: Build residual expression and execute via LINQ-to-Objects.
        // The residual expression has pre-entity Where/Take/Skip stripped
        // (they were applied during enumeration).
        var materialized = current is List<TEntity> list ? list : current.ToList();
        var residual = BuildResidualExpression(preprocessed.CleanedExpression, analysis, materialized);
        return MimironDb2QueryExecutor.CompileAndExecute<IEnumerable<TElement>>(residual);
    }

    private TResult ExecuteScalar<TResult>(Expression expression)
    {
        // Step 1: Preprocess — strip Include / EF modifiers, collect include chains.
        var preprocessed = Db2ExpressionPreprocessor.Preprocess(expression);

        // Step 2: Detect the terminal operator and get the inner expression.
        var cleanedExpr = preprocessed.CleanedExpression;

        // Verify there is a terminal operator (First, Count, Any, etc.).
        if (cleanedExpr is not MethodCallExpression { Method.DeclaringType: { } declaring } outerCall
            || declaring != typeof(Queryable)
            || outerCall.Method.Name is not (nameof(Queryable.First) or nameof(Queryable.FirstOrDefault)
                or nameof(Queryable.Single) or nameof(Queryable.SingleOrDefault)
                or nameof(Queryable.Any) or nameof(Queryable.Count) or nameof(Queryable.All)))
        {
            throw new NotSupportedException("Scalar execution requires a terminal operator.");
        }

        // Verify the terminal operator's return type is compatible with TResult.
        var terminalReturnType = outerCall.Method.ReturnType;
        if (terminalReturnType != typeof(TResult) && !typeof(TResult).IsAssignableFrom(terminalReturnType))
        {
            throw new NotSupportedException(
                $"Terminal operator '{outerCall.Method.Name}' returns {terminalReturnType.Name} " +
                $"but the expected result type is {typeof(TResult).Name}.");
        }

        // Step 3: Analyze the inner expression (without the terminal operator).
        var innerExpression = outerCall.Arguments[0];
        var analysis = AnalyzePreEntityChain(innerExpression);

        // Step 4: Expand includes and enforce include policy.
        var includeChains = preprocessed.IncludeChains.Select(static c => c.ToArray()).ToList();
        if (!preprocessed.IgnoreAutoIncludes)
            includeChains = ExpandIncludesWithAutoIncludes(includeChains);

        var includedRootMembers = includeChains
            .Where(static chain => chain.Length != 0)
            .Select(static chain => chain[0])
            .ToHashSet();

        for (var i = 0; i < analysis.PreEntityWherePredicates.Count; i++)
            Db2IncludePolicy.ThrowIfNavigationRequiresInclude(_model, includedRootMembers, analysis.PreEntityWherePredicates[i]);

        // Step 5: Enumerate entities with optimizations.
        var entities = EnumerateEntities(analysis.PreEntityWherePredicates, analysis.PreSkipCount, analysis.PreTakeCount).ToList();

        // Step 6: Apply includes.
        IEnumerable<TEntity> current = entities;
        for (var i = 0; i < includeChains.Count; i++)
            current = Db2IncludeChainExecutor.Apply(current, _model, _tableResolver, includeChains[i], _entityFactory);

        // Step 7: Build residual expression with terminal operator and execute.
        var materialized = current is List<TEntity> list ? list : current.ToList();
        var residual = BuildResidualExpression(cleanedExpr, analysis, materialized);
        return MimironDb2QueryExecutor.CompileAndExecute<TResult>(residual);
    }

    // ──────── Expression analysis ────────

    /// <summary>
    /// Walks the (Include-stripped) expression chain to extract pre-entity information:
    /// root-entity Where predicates, Take/Skip counts, Select selector, and
    /// identifies which nodes to strip for building the residual expression.
    /// </summary>
    private static PreEntityAnalysis AnalyzePreEntityChain(Expression expression)
    {
        var preEntityWhere = new List<Expression<Func<TEntity, bool>>>();
        var preWhereNodes = new List<MethodCallExpression>();
        var postEntityLambdas = new List<LambdaExpression>();
        int? preTake = null;
        int? preSkip = null;
        MethodCallExpression? preTakeNode = null;
        MethodCallExpression? preSkipNode = null;
        LambdaExpression? selectSelector = null;
        var hasPostSelectOperations = false;

        var current = expression;

        // Peel off terminal operator if present (First, Count, Any, All, Single, etc.).
        LambdaExpression? terminalPredicate = null;
        if (current is MethodCallExpression { Method.DeclaringType: { } declaring } outermost
            && declaring == typeof(Queryable)
            && outermost.Method.Name is nameof(Queryable.First) or nameof(Queryable.FirstOrDefault)
                or nameof(Queryable.Single) or nameof(Queryable.SingleOrDefault)
                or nameof(Queryable.Any) or nameof(Queryable.All) or nameof(Queryable.Count))
        {
            if (outermost.Arguments.Count == 2)
                terminalPredicate = Db2ExpressionPreprocessor.UnquoteLambda(outermost.Arguments[1]);

            current = outermost.Arguments[0];
        }

        // Collect all nodes walking outer-to-inner.
        var nodes = new List<MethodCallExpression>();
        while (current is MethodCallExpression m)
        {
            if (m.Method.DeclaringType != typeof(Queryable))
                break;

            nodes.Add(m);
            current = m.Arguments[0];
        }

        // Process nodes from inner-to-outer (data-flow order).
        // inner = closest to root = earliest in the query pipeline.
        var foundSelect = false;
        for (var i = nodes.Count - 1; i >= 0; i--)
        {
            var m = nodes[i];
            switch (m.Method.Name)
            {
                case nameof(Queryable.Where):
                    {
                        var predicate = Db2ExpressionPreprocessor.UnquoteLambda(m.Arguments[1]);
                        if (!foundSelect
                            && predicate.Parameters is { Count: 1 }
                            && predicate.Parameters[0].Type == typeof(TEntity))
                        {
                            preEntityWhere.Add((Expression<Func<TEntity, bool>>)predicate);
                            preWhereNodes.Add(m);
                        }
                        else
                        {
                            postEntityLambdas.Add(predicate);
                            if (foundSelect)
                                hasPostSelectOperations = true;
                        }

                        break;
                    }

                case nameof(Queryable.Select):
                    {
                        var selector = Db2ExpressionPreprocessor.UnquoteLambda(m.Arguments[1]);
                        if (!foundSelect)
                        {
                            selectSelector = selector;
                            foundSelect = true;
                        }
                        else
                        {
                            hasPostSelectOperations = true;
                        }

                        break;
                    }

                case nameof(Queryable.Take):
                    {
                        if (!foundSelect && m.Arguments[1] is ConstantExpression { Value: int count })
                        {
                            preTake = count;
                            preTakeNode = m;
                        }
                        else if (foundSelect)
                        {
                            hasPostSelectOperations = true;
                        }

                        break;
                    }

                case nameof(Queryable.Skip):
                    {
                        if (!foundSelect && m.Arguments[1] is ConstantExpression { Value: int count })
                        {
                            preSkip = count;
                            preSkipNode = m;
                        }
                        else if (foundSelect)
                        {
                            hasPostSelectOperations = true;
                        }

                        break;
                    }

                default:
                    hasPostSelectOperations = true;
                    break;
            }
        }

        // Terminal predicates (e.g., First(x => ...)) are always post-entity.
        if (terminalPredicate is not null)
            postEntityLambdas.Add(terminalPredicate);

        return new PreEntityAnalysis(
            PreEntityWherePredicates: preEntityWhere,
            PreWhereNodes: preWhereNodes,
            PreTakeCount: preTake,
            PreSkipCount: preSkip,
            PreTakeNode: preTakeNode,
            PreSkipNode: preSkipNode,
            SelectSelector: selectSelector,
            HasPostSelectOperations: hasPostSelectOperations,
            PostEntityLambdas: postEntityLambdas);
    }

    /// <summary>
    /// Builds a residual expression by stripping pre-entity Where/Take/Skip nodes
    /// (which have already been applied during enumeration) and substituting the root
    /// with an <see cref="EnumerableQuery{T}"/> over the materialized entities.
    /// </summary>
    private static Expression BuildResidualExpression(
        Expression cleanedExpression,
        PreEntityAnalysis analysis,
        List<TEntity> entities)
    {
        var nodesToStrip = new HashSet<Expression>(ReferenceEqualityComparer.Instance);

        foreach (var node in analysis.PreWhereNodes)
            nodesToStrip.Add(node);

        if (analysis.PreTakeNode is not null)
            nodesToStrip.Add(analysis.PreTakeNode);

        if (analysis.PreSkipNode is not null)
            nodesToStrip.Add(analysis.PreSkipNode);

        Expression residual = cleanedExpression;
        if (nodesToStrip.Count > 0)
            residual = new MimironDb2QueryExecutor.OperationStripper(nodesToStrip).Visit(residual);

        // Replace the root with entities.AsQueryable().
        var queryable = entities.AsQueryable();
        residual = new RootSubstitutor(queryable).Visit(residual);

        return residual;
    }

    // ──────── Pruned execution path ────────

    private bool TryExecuteEnumerablePruned<TProjected>(
        PreEntityAnalysis analysis,
        out IEnumerable<TProjected> result)
    {
        result = [];

        if (analysis.SelectSelector is not Expression<Func<TEntity, TProjected>> && analysis.SelectSelector is not { Parameters.Count: 1 })
            return false;

        var rowPredicates = new List<Func<TRow, bool>>();
        var requirements = new Db2SourceRequirements(_rootEntityType);
        foreach (var predicate in analysis.PreEntityWherePredicates)
        {
            if (!Db2RowPredicateCompiler.TryCompile(file, _rootEntityType, predicate, out var rowPredicate, out var predicateRequirements))
                return false;

            requirements.Columns.UnionWith(predicateRequirements.Columns);
            rowPredicates.Add(rowPredicate);
        }

        var compiled = TryCreateProjector<TProjected>(analysis.SelectSelector!);
        if (compiled is null)
            return false;

        requirements.Columns.UnionWith(compiled.Value.Requirements.Columns);

        // Pruning is only safe when we can satisfy the projection/predicate from row-level reads.
        // Virtual strings cannot be materialized from row-level reads.
        if (requirements.Columns.Any(c => c is { Kind: Db2RequiredColumnKind.String, Field.IsVirtual: true }))
            return false;

        var projected = EnumerateProjected(rowPredicates, compiled.Value.Projector, analysis.PreTakeCount);

        // Apply post-entity operations (only Take/Skip survive; Where/Include/Select are excluded).
        IEnumerable<TProjected> current = projected;

        // Post-entity Take/Skip that were NOT pre-entity (after Select) need to be applied.
        // However, AnalyzePreEntityChain only extracts pre-Select Take/Skip.
        // Any post-Select Take/Skip remain in the expression tree but would be complex to handle
        // in the pruned path, so we bail out if any exist.

        result = current;
        return true;
    }

    private (Func<TRow, TProjected> Projector, Db2SourceRequirements Requirements)? TryCreateProjector<TProjected>(LambdaExpression selector)
    {
        if (selector is not Expression<Func<TEntity, TProjected>> typed)
            return null;

        return Db2RowProjectorCompiler.TryCompile<TEntity, TProjected, TRow>(file, _rootEntityType, typed, out var projector, out var requirements) ? (projector, requirements) : null;
    }

    private IEnumerable<TProjected> EnumerateProjected<TProjected>(
        List<Func<TRow, bool>> rowPredicates,
        Func<TRow, TProjected> projector,
        int? take)
    {
        var yielded = 0;
        foreach (var row in file.EnumerateRows())
        {
            var ok = true;
            foreach (var p in rowPredicates)
            {
                if (!p(row))
                {
                    ok = false;
                    break;
                }
            }

            if (!ok)
                continue;

            yield return projector(row);

            yielded++;
            if (take.HasValue && yielded >= take.Value)
                yield break;
        }
    }

    // ──────── Entity enumeration with optimizations ────────

    private IEnumerable<TEntity> EnumerateEntities(IReadOnlyList<Expression<Func<TEntity, bool>>> predicates, int? skip, int? take)
    {
        if (take is 0)
            yield break;

        // Fast-path: if the root filter is strictly primary-key based (equality, Contains, OR),
        // resolve rows via the file's ID index instead of enumerating the whole table.
        if (predicates.Count == 1
            && TryExtractPkIds(predicates[0], _rootEntityType.PrimaryKeyMember.Name, out var ids))
        {
            var handles = new List<RowHandle>(capacity: ids.Length);

            for (var i = 0; i < ids.Length; i++)
            {
                if (!((IDb2File)file).TryGetRowHandle(ids[i], out var handle))
                    continue;

                handles.Add(handle);
            }

            if (handles.Count == 0)
                yield break;

            handles.Sort(static (a, b) =>
            {
                var section = a.SectionIndex.CompareTo(b.SectionIndex);
                if (section != 0)
                    return section;

                return a.RowIndexInSection.CompareTo(b.RowIndexInSection);
            });

            var fastSkipped = 0;
            var fastYielded = 0;

            for (var i = 0; i < handles.Count; i++)
            {
                if (skip.HasValue && fastSkipped < skip.Value)
                {
                    fastSkipped++;
                    continue;
                }

                if (take.HasValue && fastYielded >= take.Value)
                    yield break;

                yield return _materializer.Materialize(file, handles[i]);
                fastYielded++;
            }

            yield break;
        }

        var rowPredicates = new List<Func<TRow, bool>>();
        var entityPredicates = new List<Func<TEntity, bool>>();

        foreach (var predicate in predicates)
        {
            if (Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(_model, file, _tableResolver, predicate, out var navPredicate))
            {
                rowPredicates.Add(navPredicate);
                continue;
            }

            if (Db2RowPredicateCompiler.TryCompile(file, _rootEntityType, predicate, out var rowPredicate))
                rowPredicates.Add(rowPredicate);
            else
            {
                var compiled = predicate.Compile();
                entityPredicates.Add(entity =>
                {
                    try
                    {
                        return compiled(entity);
                    }
                    catch (NullReferenceException)
                    {
                        return false;
                    }
                    catch (ArgumentNullException)
                    {
                        return false;
                    }
                });
            }
        }

        var yielded = 0;
        var skipped = 0;

        switch (rowPredicates.Count)
        {
            case > 0:
                {
                    foreach (var row in file.EnumerateRows())
                    {
                        var ok = true;
                        foreach (var p in rowPredicates)
                        {
                            if (!p(row))
                            {
                                ok = false;
                                break;
                            }
                        }

                        if (!ok)
                            continue;

                        var handle = Db2RowHandleAccess.AsHandle(row);
                        var entity = _materializer.Materialize(file, handle);

                        foreach (var p in entityPredicates)
                        {
                            if (!p(entity))
                            {
                                ok = false;
                                break;
                            }
                        }

                        if (!ok)
                            continue;

                        if (skip.HasValue && skipped < skip.Value)
                        {
                            skipped++;
                            continue;
                        }

                        yield return entity;

                        yielded++;
                        if (take.HasValue && yielded >= take.Value)
                            yield break;
                    }

                    break;
                }

            default:
                {
                    foreach (var handle in file.EnumerateRowHandles())
                    {
                        var entity = _materializer.Materialize(file, handle);

                        var ok = true;
                        foreach (var p in entityPredicates)
                        {
                            if (!p(entity))
                            {
                                ok = false;
                                break;
                            }
                        }

                        if (!ok)
                            continue;

                        if (skip.HasValue && skipped < skip.Value)
                        {
                            skipped++;
                            continue;
                        }

                        yield return entity;

                        yielded++;
                        if (take.HasValue && yielded >= take.Value)
                            yield break;
                    }

                    break;
                }
        }
    }

    // ──────── Include expansion ────────

    private List<MemberInfo[]> ExpandIncludesWithAutoIncludes(List<MemberInfo[]> explicitIncludes)
    {
        var chains = explicitIncludes.Select(static c => c.ToArray()).ToList();

        foreach (var member in _model.GetAutoIncludeNavigations(typeof(TEntity)))
        {
            if (chains.Any(c => c.Length == 1 && c[0] == member))
                continue;

            chains.Add([member]);
        }

        var expanded = ExpandAutoIncludeChains(typeof(TEntity), chains);
        return expanded;
    }

    private List<MemberInfo[]> ExpandAutoIncludeChains(Type rootType, List<MemberInfo[]> startingChains)
    {
        // EF semantics: auto-includes apply transitively on included entities.
        // Prevent infinite recursion by de-duping chains and skipping cycles.
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var queue = new Queue<MemberInfo[]>(startingChains);
        var results = new List<MemberInfo[]>(startingChains.Count);

        while (queue.TryDequeue(out var chain))
        {
            var key = BuildChainKey(chain);
            if (!seen.Add(key))
                continue;

            results.Add(chain);

            if (!TryGetLeafType(rootType, chain, out var leafType))
                continue;

            var nextMembers = _model.GetAutoIncludeNavigations(leafType);
            if (nextMembers.Count == 0)
                continue;

            for (var i = 0; i < nextMembers.Count; i++)
            {
                var next = nextMembers[i];
                if (chain.Contains(next))
                    continue;

                var appended = new MemberInfo[chain.Length + 1];
                Array.Copy(chain, appended, chain.Length);
                appended[^1] = next;
                queue.Enqueue(appended);
            }
        }

        return results;
    }

    private bool TryGetLeafType(Type rootType, MemberInfo[] chain, out Type leafType)
    {
        leafType = rootType;

        for (var i = 0; i < chain.Length; i++)
        {
            var member = chain[i];

            if (_model.TryGetReferenceNavigation(leafType, member, out var referenceNav))
            {
                leafType = referenceNav.TargetClrType;
                continue;
            }

            if (_model.TryGetCollectionNavigation(leafType, member, out var collectionNav))
            {
                leafType = collectionNav.TargetClrType;
                continue;
            }

            return false;
        }

        return true;
    }

    private static string BuildChainKey(MemberInfo[] chain)
    {
        if (chain.Length == 1)
        {
            var m0 = chain[0];
            return (m0.DeclaringType?.FullName ?? "<null>") + "." + m0.Name;
        }

        return string.Join(
            "->",
            chain.Select(static m => (m.DeclaringType?.FullName ?? "<null>") + "." + m.Name));
    }

    // ──────── PK extraction ────────

    private static bool TryExtractPkIds(Expression<Func<TEntity, bool>> predicate, string pkMemberName, out int[] ids)
    {
        static Expression StripConvert(Expression e)
            => e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u ? u.Operand : e;

        ids = [];

        var raw = new List<int>();
        if (!TryExtractIdsFromExpression(StripConvert(predicate.Body), predicate.Parameters[0], pkMemberName, raw))
            return false;

        if (raw.Count == 0)
            return false;

        var seen = new HashSet<int>();
        var orderedDistinct = new List<int>(raw.Count);
        for (var i = 0; i < raw.Count; i++)
        {
            var id = raw[i];
            if (seen.Add(id))
                orderedDistinct.Add(id);
        }

        ids = [.. orderedDistinct];
        return true;

        static bool TryExtractIdsFromExpression(Expression expr, ParameterExpression param, string pkMemberName, List<int> ids)
        {
            expr = StripConvert(expr);

            switch (expr)
            {
                case BinaryExpression { NodeType: ExpressionType.Equal } eq:
                    {
                        var left = StripConvert(eq.Left);
                        var right = StripConvert(eq.Right);

                        if (IsKeyAccess(left, param, pkMemberName) && TryEvaluateInt(right, param, out var rightId))
                        {
                            ids.Add(rightId);
                            return true;
                        }

                        if (IsKeyAccess(right, param, pkMemberName) && TryEvaluateInt(left, param, out var leftId))
                        {
                            ids.Add(leftId);
                            return true;
                        }

                        return false;
                    }

                case BinaryExpression { NodeType: ExpressionType.OrElse } or:
                    return TryExtractIdsFromExpression(or.Left, param, pkMemberName, ids)
                           && TryExtractIdsFromExpression(or.Right, param, pkMemberName, ids);

                case MethodCallExpression call:
                    return TryExtractContains(call, param, pkMemberName, ids);

                default:
                    return false;
            }
        }

        static bool TryExtractContains(MethodCallExpression call, ParameterExpression param, string pkMemberName, List<int> ids)
        {
            if (!string.Equals(call.Method.Name, nameof(Enumerable.Contains), StringComparison.Ordinal))
                return false;

            Expression? collectionExpr = null;
            Expression? valueExpr = null;

            if (call.Method.DeclaringType == typeof(Enumerable) && call.Arguments.Count == 2)
            {
                collectionExpr = call.Arguments[0];
                valueExpr = call.Arguments[1];
            }
            else if (call.Object is not null && call.Arguments.Count == 1)
            {
                collectionExpr = call.Object;
                valueExpr = call.Arguments[0];
            }
            else if (call.Object is null && call.Arguments.Count == 2)
            {
                collectionExpr = call.Arguments[0];
                valueExpr = call.Arguments[1];
            }

            if (collectionExpr is null || valueExpr is null)
                return false;

            valueExpr = StripConvert(valueExpr);
            if (!IsKeyAccess(valueExpr, param, pkMemberName))
                return false;

            if (!TryEvaluateIntSequence(collectionExpr, param, out var values))
                return false;

            for (var i = 0; i < values.Count; i++)
                ids.Add(values[i]);

            return true;
        }

        static bool IsKeyAccess(Expression expr, ParameterExpression param, string pkMemberName)
        {
            if (expr is MemberExpression { Member: { Name: var name }, Expression: var instance } && instance == param && name == pkMemberName)
                return true;

            if (expr is MethodCallExpression { Method: { Name: "Property", DeclaringType: { Name: "EF", Namespace: "Microsoft.EntityFrameworkCore" } }, Arguments: [var entityExpr, var nameExpr] })
            {
                if (entityExpr == param && nameExpr is ConstantExpression { Value: string s } && s == pkMemberName)
                    return true;
            }

            return false;
        }

        static bool TryEvaluateIntSequence(Expression expr, ParameterExpression param, out IReadOnlyList<int> values)
        {
            expr = StripConvert(expr);

            if (expr is NewArrayExpression newArray)
            {
                var list = new List<int>(newArray.Expressions.Count);
                for (var i = 0; i < newArray.Expressions.Count; i++)
                {
                    if (!TryEvaluateInt(newArray.Expressions[i], param, out var value))
                    {
                        values = [];
                        return false;
                    }

                    list.Add(value);
                }

                values = list;
                return true;
            }

            if (ReferencesParameter(expr, param))
            {
                values = [];
                return false;
            }

            try
            {
                var converted = Expression.Convert(expr, typeof(object));
                var fn = Expression.Lambda<Func<object>>(converted).Compile();
                var obj = fn();
                if (obj is IEnumerable<int> ints)
                {
                    values = ints as IReadOnlyList<int> ?? ints.ToArray();
                    return true;
                }
            }
            catch
            {
                // ignore
            }

            values = [];
            return false;
        }

        static bool TryEvaluateInt(Expression expr, ParameterExpression param, out int value)
        {
            expr = StripConvert(expr);

            if (expr is ConstantExpression { Value: int i })
            {
                value = i;
                return true;
            }

            if (ReferencesParameter(expr, param))
            {
                value = 0;
                return false;
            }

            try
            {
                var fn = Expression.Lambda<Func<int>>(Expression.Convert(expr, typeof(int))).Compile();
                value = fn();
                return true;
            }
            catch
            {
                value = 0;
                return false;
            }
        }

        static bool ReferencesParameter(Expression expr, ParameterExpression param)
        {
            var visitor = new ParameterReferenceVisitor(param);
            visitor.Visit(expr);
            return visitor.Found;
        }
    }

    private sealed class ParameterReferenceVisitor(ParameterExpression parameter) : ExpressionVisitor
    {
        private readonly ParameterExpression _parameter = parameter;
        public bool Found { get; private set; }

        public override Expression? Visit(Expression? node)
        {
            if (Found || node is null)
                return node;

            return base.Visit(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == _parameter)
                Found = true;

            return node;
        }
    }

    private sealed class ParameterReplaceVisitor(ParameterExpression from, Expression to) : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node)
            => node == from ? to : base.VisitParameter(node);
    }

    // ──────── Root substitution ────────

    /// <summary>
    /// Replaces the root expression (a Db2Queryable constant or IQueryable constant)
    /// with a LINQ-to-Objects EnumerableQuery.
    /// </summary>
    private sealed class RootSubstitutor(IQueryable replacement) : ExpressionVisitor
    {
        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Value is IQueryable q && q.ElementType == replacement.ElementType
                && node.Value is Db2Queryable<TEntity>)
            {
                return Expression.Constant(replacement, typeof(IQueryable<TEntity>));
            }

            return base.VisitConstant(node);
        }
    }

    // ──────── Supporting types ────────

    private sealed record PreEntityAnalysis(
        IReadOnlyList<Expression<Func<TEntity, bool>>> PreEntityWherePredicates,
        IReadOnlyList<MethodCallExpression> PreWhereNodes,
        int? PreTakeCount,
        int? PreSkipCount,
        MethodCallExpression? PreTakeNode,
        MethodCallExpression? PreSkipNode,
        LambdaExpression? SelectSelector,
        bool HasPostSelectOperations,
        IReadOnlyList<LambdaExpression> PostEntityLambdas);

    private static bool TryGetEnumerableElementType(Type type, out Type elementType)
    {
        // Special-case: string implements IEnumerable<char>, but for query results we treat it as a scalar.
        if (type == typeof(string))
        {
            elementType = typeof(string).BaseType!;
            return false;
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
        {
            elementType = type.GetGenericArguments()[0];
            return true;
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IQueryable<>))
        {
            elementType = type.GetGenericArguments()[0];
            return true;
        }

        var ienum = type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
        if (ienum is not null)
        {
            elementType = ienum.GetGenericArguments()[0];
            return true;
        }

        elementType = typeof(string).BaseType!;
        return false;
    }
}
