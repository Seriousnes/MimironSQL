using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Model;
using MimironSQL.Formats;

using System.Collections.Concurrent;
using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using MimironSQL.Extensions;

namespace MimironSQL.Db2.Query;

internal sealed class Db2QueryProvider<TEntity, TRow>(
    IDb2File<TRow> file,
    Db2Model model,
    Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver) : IQueryProvider
    where TRow : struct, IRowHandle
{
    private static readonly ConcurrentDictionary<Type, Func<Db2QueryProvider<TEntity, TRow>, Expression, IEnumerable>> ExecuteEnumerableDelegates = new();

    private readonly Db2EntityType _rootEntityType = model.GetEntityType(typeof(TEntity));
    private readonly Db2EntityMaterializer<TEntity, TRow> _materializer = new(model.GetEntityType(typeof(TEntity)));
    private readonly Db2Model _model = model;
    private readonly Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> _tableResolver = tableResolver;

    internal TEntity Materialize(RowHandle handle) => _materializer.Materialize(file, handle);

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

    private IEnumerable<TElement> ExecuteEnumerable<TElement>(Expression expression)
    {
        var pipeline = Db2QueryPipeline.Parse(expression);

        var ops = pipeline.Operations.ToList();

        var selectIndex = ops.FindIndex(op => op is Db2SelectOperation);
        if (selectIndex < 0)
            selectIndex = ops.Count;

        var preEntityWhere = ops
            .Take(selectIndex)
            .OfType<Db2WhereOperation>()
            .Where(op => op is { Predicate.Parameters.Count: 1 } && op.Predicate.Parameters[0].Type == typeof(TEntity))
            .Select(op => (Expression<Func<TEntity, bool>>)op.Predicate)
            .ToList();

        var selectOp = selectIndex < ops.Count ? ops[selectIndex] as Db2SelectOperation : null;

        var preTake = ops
            .Take(selectIndex)
            .OfType<Db2TakeOperation>()
            .FirstOrDefault();

        var preSkip = ops
            .Take(selectIndex)
            .OfType<Db2SkipOperation>()
            .FirstOrDefault();

        var stopAfter = preTake?.Count;
        var skipBeforeTake = preSkip?.Count;

        var postOps = new List<Db2QueryOperation>(ops.Count);
        for (var i = 0; i < ops.Count; i++)
        {
            var op = ops[i];

            if (op == preTake)
                continue;

            if (op == preSkip)
                continue;

            if (i < selectIndex && op is Db2WhereOperation w && w is { Predicate.Parameters.Count: 1 } && w.Predicate.Parameters[0].Type == typeof(TEntity))
            {
                // Root entity predicates apply before Include/Take/Skip regardless of their position
                // (so Take/Skip are not applied before filtering).
                continue;
            }

            postOps.Add(op);
        }

        var includeOps = postOps.OfType<Db2IncludeOperation>().ToList();

        var includedRootMembers = includeOps
            .Where(static op => op.Members.Count != 0)
            .Select(static op => op.Members[0])
            .ToHashSet();

        // Enforce the explicit Include requirement for any root navigation usage.
        for (var i = 0; i < preEntityWhere.Count; i++)
            Db2IncludePolicy.ThrowIfNavigationRequiresInclude(_model, includedRootMembers, preEntityWhere[i]);

        if (selectOp is not null)
        {
            var canAttemptPrune =
                selectOp.Selector is { Parameters.Count: 1 } &&
                selectOp.Selector.Parameters[0].Type == typeof(TEntity) &&
                selectOp.Selector.ReturnType == typeof(TElement) &&
                !Db2IncludePolicy.UsesRootNavigation(_model, selectOp.Selector) &&
                postOps.All(op => op is not Db2WhereOperation && op is not Db2IncludeOperation) &&
                postOps.Count(op => op is Db2SelectOperation) <= 1;

            if (canAttemptPrune)
            {
                if (TryExecuteEnumerablePruned<TElement>(preEntityWhere, stopAfter, selectOp.Selector, postOps, out var pruned))
                    return pruned;
            }
        }

        IEnumerable<TEntity> baseEntities = EnumerateEntities(preEntityWhere, skipBeforeTake, stopAfter);

        IEnumerable current = baseEntities;
        var currentElementType = typeof(TEntity);

        // Include must apply to the root entity sequence. Once Select changes the element type,
        // Include no longer has a well-defined meaning for this provider.
        {
            var elementType = typeof(TEntity);
            for (var i = 0; i < postOps.Count; i++)
            {
                var op = postOps[i];
                switch (op)
                {
                    case Db2SelectOperation select:
                        elementType = select.Selector.ReturnType;
                        break;
                    case Db2IncludeOperation when elementType != typeof(TEntity):
                        throw new NotSupportedException("Include must appear before Select for this provider.");
                }
            }
        }

        foreach (var op in postOps)
        {
            Db2IncludePolicy.ThrowIfNavigationRequiresInclude(_model, includedRootMembers, op);
        }

        // Apply includes first so navigations can be used in predicates/projections.
        for (var i = 0; i < includeOps.Count; i++)
        {
            current = Db2IncludeChainExecutor.Apply((IEnumerable<TEntity>)current, _model, _tableResolver, includeOps[i].Members);
        }

        for (var opIndex = 0; opIndex < postOps.Count; opIndex++)
        {
            var op = postOps[opIndex];
            switch (op)
            {
                case Db2WhereOperation where:
                    current = ApplyWhere(current, currentElementType, where.Predicate);
                    break;
                case Db2SelectOperation select:
                    current = ApplySelect(current, currentElementType, select.Selector);
                    currentElementType = select.Selector.ReturnType;
                    break;
                case Db2IncludeOperation:
                    // Applied up-front.
                    break;
                case Db2TakeOperation take:
                    current = ApplyTake(current, currentElementType, take.Count);
                    break;
                case Db2SkipOperation skip:
                    current = ApplySkip(current, currentElementType, skip.Count);
                    break;
                default:
                    throw new NotSupportedException($"Unsupported query operation: {op.GetType().Name}.");
            }
        }

        return (IEnumerable<TElement>)current;
    }

    private bool TryExecuteEnumerablePruned<TProjected>(
        IList<Expression<Func<TEntity, bool>>> preEntityWhere,
        int? stopAfter,
        LambdaExpression selector,
        List<Db2QueryOperation> postOps,
        out IEnumerable<TProjected> result)
    {
        result = [];

        if (selector is not Expression<Func<TEntity, TProjected>> && selector.Parameters is not { Count: 1 })
            return false;

        var rowPredicates = new List<Func<TRow, bool>>();
        var requirements = new Db2SourceRequirements(_rootEntityType);
        foreach (var predicate in preEntityWhere)
        {
            if (!Db2RowPredicateCompiler.TryCompile(file, _rootEntityType, predicate, out var rowPredicate, out var predicateRequirements))
                return false;

            requirements.Columns.UnionWith(predicateRequirements.Columns);
            rowPredicates.Add(rowPredicate);
        }

        var compiled = TryCreateProjector<TProjected>(selector);
        if (compiled is null)
            return false;

        requirements.Columns.UnionWith(compiled.Value.Requirements.Columns);

        // Pruning is only safe when we can satisfy the projection/predicate from row-level reads.
        // Virtual strings cannot be materialized from row-level reads.
        if (requirements.Columns.Any(c => c is { Kind: Db2RequiredColumnKind.String, Field.IsVirtual: true }))
            return false;

        var projected = EnumerateProjected(rowPredicates, compiled.Value.Projector, stopAfter);

        IEnumerable current = projected;
        var currentElementType = typeof(TProjected);

        foreach (var op in postOps)
        {
            switch (op)
            {
                case Db2SelectOperation:
                    continue;
                case Db2IncludeOperation:
                    return false;
                case Db2TakeOperation take:
                    current = ApplyTake(current, currentElementType, take.Count);
                    break;
                case Db2SkipOperation skip:
                    current = ApplySkip(current, currentElementType, skip.Count);
                    break;
                default:
                    return false;
            }
        }

        result = (IEnumerable<TProjected>)current;
        return true;
    }

    private bool TryExecuteNavigationProjectionPruned<TProjected>(
        IList<Expression<Func<TEntity, bool>>> preEntityWhere,
        int? stopAfter,
        LambdaExpression selector,
        List<Db2QueryOperation> postOps,
        out IEnumerable<TProjected> result)
    {
        // Navigation projection pruning is intentionally disabled.
        // For EF-like semantics we require explicit Include for navigation access.
        result = [];
        return false;
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

    private IEnumerable<TProjected> EnumerateNavigationProjected<TProjected>(
        List<Func<TRow, bool>> rowPredicates,
        int? take,
        IReadOnlyList<Db2NavigationMemberAccessPlan> navAccesses,
        LambdaExpression selector)
    {
        IEnumerable<TRow> FilteredRows()
        {
            var yielded = 0;
            foreach (var row in file.EnumerateRows())
            {
                if (!rowPredicates.All(p => p(row)))
                    continue;

                yield return row;

                yielded++;
                if (take.HasValue && yielded >= take.Value)
                    yield break;
            }
        }

        return Db2NavigationRowProjector.ProjectFromRows<TProjected, TRow>(
            file,
            FilteredRows(),
            _rootEntityType,
            _model,
            _tableResolver,
            [.. navAccesses],
            selector,
            null);
    }

    private TResult ExecuteScalar<TResult>(Expression expression)
    {
        var pipeline = Db2QueryPipeline.Parse(expression);

        if (pipeline.FinalOperator == Db2FinalOperator.None)
            throw new NotSupportedException("Scalar execution requires a terminal operator.");

        if (pipeline.FinalElementType != typeof(TResult))
            throw new NotSupportedException($"Scalar execution expected result type {typeof(TResult).FullName} but pipeline produced {pipeline.FinalElementType.FullName}.");

        if (!TryGetEnumerableElementType(pipeline.ExpressionWithoutFinalOperator.Type, out var sequenceElementType))
            throw new NotSupportedException("Unable to determine sequence element type for scalar execution.");

        switch (pipeline.FinalOperator)
        {
            case Db2FinalOperator.First:
                {
                    var sequence = ExecuteEnumerable<TResult>(pipeline.ExpressionWithoutFinalOperator);
                    return Enumerable.First(sequence);
                }
            case Db2FinalOperator.FirstOrDefault:
                {
                    var sequence = ExecuteEnumerable<TResult>(pipeline.ExpressionWithoutFinalOperator);
                    return Enumerable.FirstOrDefault(sequence)!;
                }
            case Db2FinalOperator.Single:
                {
                    var sequence = ExecuteEnumerable<TResult>(pipeline.ExpressionWithoutFinalOperator);
                    return Enumerable.Single(sequence);
                }
            case Db2FinalOperator.SingleOrDefault:
                {
                    var sequence = ExecuteEnumerable<TResult>(pipeline.ExpressionWithoutFinalOperator);
                    return Enumerable.SingleOrDefault(sequence)!;
                }
            case Db2FinalOperator.Any:
                {
                    if (typeof(TResult) != typeof(bool))
                        throw new NotSupportedException("Queryable.Any must return bool.");

                    var sequence = GetExecuteEnumerableDelegate(sequenceElementType)(this, pipeline.ExpressionWithoutFinalOperator);
                    var any = EnumerableDispatch.GetAny(sequenceElementType)(sequence);
                    return Unsafe.As<bool, TResult>(ref any);
                }
            case Db2FinalOperator.Count:
                {
                    if (typeof(TResult) != typeof(int))
                        throw new NotSupportedException("Queryable.Count must return int.");

                    var sequence = GetExecuteEnumerableDelegate(sequenceElementType)(this, pipeline.ExpressionWithoutFinalOperator);
                    var count = EnumerableDispatch.GetCount(sequenceElementType)(sequence);
                    return Unsafe.As<int, TResult>(ref count);
                }
            case Db2FinalOperator.All:
                {
                    if (typeof(TResult) != typeof(bool))
                        throw new NotSupportedException("Queryable.All must return bool.");

                    if (pipeline.FinalPredicate is null)
                        throw new NotSupportedException("Queryable.All requires a predicate for this provider.");

                    var sequence = GetExecuteEnumerableDelegate(sequenceElementType)(this, pipeline.ExpressionWithoutFinalOperator);
                    var predicate = pipeline.FinalPredicate.Compile();
                    var all = EnumerableDispatch.GetAll(sequenceElementType)(sequence, predicate);
                    return Unsafe.As<bool, TResult>(ref all);
                }
            default:
                throw new NotSupportedException($"Unsupported scalar operator: {pipeline.FinalOperator}.");
        }
    }

    private IEnumerable<TEntity> EnumerateEntities(IList<Expression<Func<TEntity, bool>>> predicates, int? skip, int? take)
    {
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

    private static IEnumerable ApplyWhere(IEnumerable source, Type sourceElementType, LambdaExpression predicate)
    {
        var typedPredicate = predicate.Compile();
        return EnumerableDispatch.GetWhere(sourceElementType)(source, typedPredicate);
    }

    private static IEnumerable ApplySelect(IEnumerable source, Type sourceElementType, LambdaExpression selector)
    {
        var resultType = selector.ReturnType;
        var typedSelector = selector.Compile();

        return EnumerableDispatch.GetSelect(sourceElementType, resultType)(source, typedSelector);
    }

    private static IEnumerable ApplyTake(IEnumerable source, Type sourceElementType, int count)
    {

        return EnumerableDispatch.GetTake(sourceElementType)(source, count);
    }

    private static IEnumerable ApplySkip(IEnumerable source, Type sourceElementType, int count)
    {
        return EnumerableDispatch.GetSkip(sourceElementType)(source, count);
    }

    private static class EnumerableDispatch
    {
        private static readonly ConcurrentDictionary<Type, Func<IEnumerable, Delegate, IEnumerable>> WhereDelegates = new();
        private static readonly ConcurrentDictionary<(Type Source, Type Result), Func<IEnumerable, Delegate, IEnumerable>> SelectDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<IEnumerable, int, IEnumerable>> TakeDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<IEnumerable, int, IEnumerable>> SkipDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<IEnumerable, bool>> AnyDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<IEnumerable, int>> CountDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<IEnumerable, Delegate, bool>> AllDelegates = new();

        public static Func<IEnumerable, Delegate, IEnumerable> GetWhere(Type elementType)
            => WhereDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(EnumerableDispatch)
                    .GetMethod(nameof(WhereImpl), BindingFlags.Static | BindingFlags.NonPublic)!
                    .MakeGenericMethod(elementType);

                return m.CreateDelegate<Func<IEnumerable, Delegate, IEnumerable>>();
            });

        public static Func<IEnumerable, Delegate, IEnumerable> GetSelect(Type sourceType, Type resultType)
            => SelectDelegates.GetOrAdd((sourceType, resultType), static key =>
            {
                var m = typeof(EnumerableDispatch)
                    .GetMethod(nameof(SelectImpl), BindingFlags.Static | BindingFlags.NonPublic)!
                    .MakeGenericMethod(key.Source, key.Result);

                return m.CreateDelegate<Func<IEnumerable, Delegate, IEnumerable>>();
            });

        public static Func<IEnumerable, int, IEnumerable> GetTake(Type elementType)
            => TakeDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(EnumerableDispatch)
                    .GetMethod(nameof(TakeImpl), BindingFlags.Static | BindingFlags.NonPublic)!
                    .MakeGenericMethod(elementType);

                return m.CreateDelegate<Func<IEnumerable, int, IEnumerable>>();
            });

        public static Func<IEnumerable, int, IEnumerable> GetSkip(Type elementType)
            => SkipDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(EnumerableDispatch)
                    .GetMethod(nameof(SkipImpl), BindingFlags.Static | BindingFlags.NonPublic)!
                    .MakeGenericMethod(elementType);

                return m.CreateDelegate<Func<IEnumerable, int, IEnumerable>>();
            });

        public static Func<IEnumerable, bool> GetAny(Type elementType)
            => AnyDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(EnumerableDispatch)
                    .GetMethod(nameof(AnyImpl), BindingFlags.Static | BindingFlags.NonPublic)!
                    .MakeGenericMethod(elementType);

                return m.CreateDelegate<Func<IEnumerable, bool>>();
            });

        public static Func<IEnumerable, int> GetCount(Type elementType)
            => CountDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(EnumerableDispatch)
                    .GetMethod(nameof(CountImpl), BindingFlags.Static | BindingFlags.NonPublic)!
                    .MakeGenericMethod(elementType);

                return m.CreateDelegate<Func<IEnumerable, int>>();
            });

        public static Func<IEnumerable, Delegate, bool> GetAll(Type elementType)
            => AllDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(EnumerableDispatch)
                    .GetMethod(nameof(AllImpl), BindingFlags.Static | BindingFlags.NonPublic)!
                    .MakeGenericMethod(elementType);

                return m.CreateDelegate<Func<IEnumerable, Delegate, bool>>();
            });

        private static IEnumerable WhereImpl<T>(IEnumerable source, Delegate predicate)
        {
            var typed = (Func<T, bool>)predicate;

            return Enumerable.Where((IEnumerable<T>)source, x =>
            {
                try
                {
                    return typed(x);
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

        private static IEnumerable SelectImpl<TSource, TResult>(IEnumerable source, Delegate selector)
            => Enumerable.Select((IEnumerable<TSource>)source, (Func<TSource, TResult>)selector);

        private static IEnumerable TakeImpl<T>(IEnumerable source, int count)
            => Enumerable.Take((IEnumerable<T>)source, count);

        private static IEnumerable SkipImpl<T>(IEnumerable source, int count)
            => Enumerable.Skip((IEnumerable<T>)source, count);

        private static bool AnyImpl<T>(IEnumerable source)
            => Enumerable.Any((IEnumerable<T>)source);

        private static int CountImpl<T>(IEnumerable source)
            => Enumerable.Count((IEnumerable<T>)source);

        private static bool AllImpl<T>(IEnumerable source, Delegate predicate)
            => Enumerable.All((IEnumerable<T>)source, (Func<T, bool>)predicate);
    }

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
