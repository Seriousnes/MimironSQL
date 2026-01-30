using MimironSQL.Db2;
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
    Db2TableSchema schema,
    Db2Model model,
    Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver) : IQueryProvider
    where TRow : struct
{
    private static readonly ConcurrentDictionary<Type, Func<Db2QueryProvider<TEntity, TRow>, Expression, IEnumerable>> ExecuteEnumerableDelegates = new();
    private static readonly ConcurrentDictionary<Type, Func<Db2QueryProvider<TEntity, TRow>, IEnumerable<TEntity>, LambdaExpression, IEnumerable<TEntity>>> IncludeDelegates = new();
    private static readonly ConcurrentDictionary<(Type NavigationType, Type TargetType), Func<Db2QueryProvider<TEntity, TRow>, IEnumerable<TEntity>, MemberInfo, string, Db2CollectionNavigation, IEnumerable<TEntity>>> ForeignKeyArrayToPrimaryKeyIncludeDelegates = new();
    private static readonly ConcurrentDictionary<(Type NavigationType, Type TargetType), Func<Db2QueryProvider<TEntity, TRow>, IEnumerable<TEntity>, MemberInfo, string, Db2CollectionNavigation, IEnumerable<TEntity>>> DependentForeignKeyToPrimaryKeyIncludeDelegates = new();

    private readonly Db2EntityMaterializer<TEntity, TRow> _materializer = new(schema);
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
        var factory = (Func<IQueryProvider, Expression, IQueryable>)generic.CreateDelegate(typeof(Func<IQueryProvider, Expression, IQueryable>));
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

            return (Func<Db2QueryProvider<TEntity, TRow>, Expression, IEnumerable>)method.CreateDelegate(
                typeof(Func<Db2QueryProvider<TEntity, TRow>, Expression, IEnumerable>));
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

        var stopAfter = preTake?.Count;

        var postOps = new List<Db2QueryOperation>(ops.Count);
        for (var i = 0; i < ops.Count; i++)
        {
            var op = ops[i];

            if (op == preTake)
                continue;

            if (i < selectIndex && op is Db2WhereOperation w && w is { Predicate.Parameters.Count: 1 } && w.Predicate.Parameters[0].Type == typeof(TEntity))
                continue;

            postOps.Add(op);
        }

        if (selectOp is not null)
        {
            var canAttemptPrune =
                selectOp.Selector is { Parameters.Count: 1 } &&
                selectOp.Selector.Parameters[0].Type == typeof(TEntity) &&
                selectOp.Selector.ReturnType == typeof(TElement) &&
                !SelectorUsesNavigation(selectOp.Selector) &&
                postOps.All(op => op is not Db2WhereOperation && op is not Db2IncludeOperation) &&
                postOps.Count(op => op is Db2SelectOperation) <= 1;

            if (canAttemptPrune)
            {
                if (TryExecuteEnumerablePruned<TElement>(preEntityWhere, stopAfter, selectOp.Selector, postOps, out var pruned))
                    return pruned;
            }

            var canAttemptNavigationPrune =
                selectOp.Selector is { Parameters.Count: 1 } &&
                selectOp.Selector.Parameters[0].Type == typeof(TEntity) &&
                selectOp.Selector.ReturnType == typeof(TElement) &&
                SelectorUsesNavigation(selectOp.Selector) &&
                postOps.All(op => op is not Db2WhereOperation && op is not Db2IncludeOperation) &&
                postOps.Count(op => op is Db2SelectOperation) <= 1;

            if (canAttemptNavigationPrune && TryExecuteNavigationProjectionPruned<TElement>(preEntityWhere, stopAfter, selectOp.Selector, postOps, out var navPruned))
                return navPruned;
        }

        IEnumerable<TEntity> baseEntities = EnumerateEntities(preEntityWhere, stopAfter);

        IEnumerable current = baseEntities;
        var currentElementType = typeof(TEntity);

        for (var opIndex = 0; opIndex < postOps.Count; opIndex++)
        {
            var op = postOps[opIndex];
            switch (op)
            {
                case Db2WhereOperation where:
                    current = ApplyWhere(current, currentElementType, where.Predicate);
                    break;
                case Db2SelectOperation select:
                    if (currentElementType == typeof(TEntity))
                    {
                        var navAccesses = Db2NavigationQueryTranslator.GetNavigationAccesses<TEntity>(_model, select.Selector);
                        if (navAccesses is { Count: not 0 } && select.Selector is Expression<Func<TEntity, TElement>> typedSelector)
                        {
                            int? take = null;
                            if (opIndex + 1 < postOps.Count && postOps[opIndex + 1] is Db2TakeOperation nextTake)
                            {
                                take = nextTake.Count;
                                opIndex++;
                            }

                            current = Db2BatchedNavigationProjector.Project(
                                (IEnumerable<TEntity>)current,
                                _model,
                                _tableResolver,
                                [.. navAccesses],
                                typedSelector,
                                take);

                            currentElementType = typeof(TElement);
                            break;
                        }

                        var navMembers = navAccesses
                            .Select(a => a.Join.Navigation.NavigationMember)
                            .Distinct();

                        foreach (var navMember in navMembers)
                        {
                            var navLambda = CreateNavigationLambda(navMember);
                            current = ApplyInclude((IEnumerable<TEntity>)current, navLambda);
                        }
                    }

                    current = ApplySelect(current, currentElementType, select.Selector);
                    currentElementType = select.Selector.ReturnType;
                    break;
                case Db2IncludeOperation:
                    if (currentElementType != typeof(TEntity))
                        throw new NotSupportedException("Include must appear before Select for this provider.");

                    current = ApplyInclude((IEnumerable<TEntity>)current, ((Db2IncludeOperation)op).Navigation);
                    break;
                case Db2TakeOperation take:
                    current = ApplyTake(current, currentElementType, take.Count);
                    break;
                default:
                    throw new NotSupportedException($"Unsupported query operation: {op.GetType().Name}.");
            }
        }

        bool SelectorUsesNavigation(LambdaExpression selector)
            => Db2NavigationQueryTranslator.GetNavigationAccesses<TEntity>(_model, selector).Count != 0;

        LambdaExpression CreateNavigationLambda(MemberInfo navMember)
        {
            var entity = Expression.Parameter(typeof(TEntity), "entity");
            Expression access = navMember switch
            {
                PropertyInfo p => Expression.Property(entity, p),
                FieldInfo f => Expression.Field(entity, f),
                _ => throw new InvalidOperationException($"Unexpected navigation member type: {navMember.GetType().FullName}"),
            };

            return Expression.Lambda(access, entity);
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
        result = Array.Empty<TProjected>();

        if (selector is not Expression<Func<TEntity, TProjected>> && selector.Parameters is not { Count: 1 })
            return false;

        var rowPredicates = new List<Func<TRow, bool>>();
        var requirements = new Db2SourceRequirements(schema, typeof(TEntity));
        foreach (var predicate in preEntityWhere)
        {
            if (!Db2RowPredicateCompiler.TryCompile(file, schema, predicate, out var rowPredicate, out var predicateRequirements))
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
        result = Array.Empty<TProjected>();

        if (selector.Parameters is not { Count: 1 })
            return false;

        var navAccesses = Db2NavigationQueryTranslator.GetNavigationAccesses<TEntity>(_model, selector);
        if (navAccesses is not { Count: not 0 })
            return false;

        var rowPredicates = new List<Func<TRow, bool>>();
        var rootRequirements = new Db2SourceRequirements(schema, typeof(TEntity));

        foreach (var predicate in preEntityWhere)
        {
            if (!Db2RowPredicateCompiler.TryCompile(file, schema, predicate, out var rowPredicate, out var predicateRequirements))
                return false;

            rootRequirements.Columns.UnionWith(predicateRequirements.Columns);
            rowPredicates.Add(rowPredicate);
        }

        foreach (var access in navAccesses)
        {
            rootRequirements.Columns.UnionWith(access.RootRequirements.Columns);
        }

        if (rootRequirements.Columns.Any(c => c is { Kind: Db2RequiredColumnKind.String, Field.IsVirtual: true }))
            return false;

        var projected = EnumerateNavigationProjected<TProjected>(rowPredicates, stopAfter, navAccesses, selector);

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
                default:
                    return false;
            }
        }

        result = (IEnumerable<TProjected>)current;
        return true;
    }

    private (Func<TRow, TProjected> Projector, Db2SourceRequirements Requirements)? TryCreateProjector<TProjected>(LambdaExpression selector)
    {
        if (selector is not Expression<Func<TEntity, TProjected>> typed)
            return null;

        return Db2RowProjectorCompiler.TryCompile<TEntity, TProjected, TRow>(schema, typed, out var projector, out var requirements)
            ? (projector, requirements)
            : null;
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
            FilteredRows(),
            schema,
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

    private IEnumerable<TEntity> EnumerateEntities(IList<Expression<Func<TEntity, bool>>> predicates, int? take)
    {
        var rowPredicates = new List<Func<TRow, bool>>();
        var entityPredicates = new List<Func<TEntity, bool>>();

        foreach (var predicate in predicates)
        {
            if (Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate<TEntity, TRow>(_model, file, schema, _tableResolver, predicate, out var navPredicate))
            {
                rowPredicates.Add(navPredicate);
                continue;
            }

            if (Db2RowPredicateCompiler.TryCompile(file, schema, predicate, out var rowPredicate))
                rowPredicates.Add(rowPredicate);
            else
                entityPredicates.Add(predicate.Compile());
        }

        var yielded = 0;
        
        if (rowPredicates.Count > 0)
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

                var id = row.Get<int>(Db2VirtualFieldIndex.Id);
                if (!file.TryGetRowHandle(id, out var handle))
                    continue;

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

                yield return entity;

                yielded++;
                if (take.HasValue && yielded >= take.Value)
                    yield break;
            }
        }
        else
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

                yield return entity;

                yielded++;
                if (take.HasValue && yielded >= take.Value)
                    yield break;
            }
        }
    }

    private static IEnumerable ApplyWhere(IEnumerable source, Type sourceElementType, LambdaExpression predicate)
    {
        var typedPredicate = predicate.Compile();
        return (IEnumerable)EnumerableDispatch.GetWhere(sourceElementType)(source, typedPredicate);
    }

    private static IEnumerable ApplySelect(IEnumerable source, Type sourceElementType, LambdaExpression selector)
    {
        var resultType = selector.ReturnType;
        var typedSelector = selector.Compile();

        return (IEnumerable)EnumerableDispatch.GetSelect(sourceElementType, resultType)(source, typedSelector);
    }

    private static IEnumerable ApplyTake(IEnumerable source, Type sourceElementType, int count)
    {

        return (IEnumerable)EnumerableDispatch.GetTake(sourceElementType)(source, count);
    }

    private static class EnumerableDispatch
    {
        private static readonly ConcurrentDictionary<Type, Func<IEnumerable, Delegate, IEnumerable>> WhereDelegates = new();
        private static readonly ConcurrentDictionary<(Type Source, Type Result), Func<IEnumerable, Delegate, IEnumerable>> SelectDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<IEnumerable, int, IEnumerable>> TakeDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<IEnumerable, bool>> AnyDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<IEnumerable, int>> CountDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<IEnumerable, Delegate, bool>> AllDelegates = new();

        public static Func<IEnumerable, Delegate, IEnumerable> GetWhere(Type elementType)
            => WhereDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(EnumerableDispatch)
                    .GetMethod(nameof(WhereImpl), BindingFlags.Static | BindingFlags.NonPublic)!
                    .MakeGenericMethod(elementType);

                return (Func<IEnumerable, Delegate, IEnumerable>)m.CreateDelegate(typeof(Func<IEnumerable, Delegate, IEnumerable>));
            });

        public static Func<IEnumerable, Delegate, IEnumerable> GetSelect(Type sourceType, Type resultType)
            => SelectDelegates.GetOrAdd((sourceType, resultType), static key =>
            {
                var m = typeof(EnumerableDispatch)
                    .GetMethod(nameof(SelectImpl), BindingFlags.Static | BindingFlags.NonPublic)!
                    .MakeGenericMethod(key.Source, key.Result);

                return (Func<IEnumerable, Delegate, IEnumerable>)m.CreateDelegate(typeof(Func<IEnumerable, Delegate, IEnumerable>));
            });

        public static Func<IEnumerable, int, IEnumerable> GetTake(Type elementType)
            => TakeDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(EnumerableDispatch)
                    .GetMethod(nameof(TakeImpl), BindingFlags.Static | BindingFlags.NonPublic)!
                    .MakeGenericMethod(elementType);

                return (Func<IEnumerable, int, IEnumerable>)m.CreateDelegate(typeof(Func<IEnumerable, int, IEnumerable>));
            });

        public static Func<IEnumerable, bool> GetAny(Type elementType)
            => AnyDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(EnumerableDispatch)
                    .GetMethod(nameof(AnyImpl), BindingFlags.Static | BindingFlags.NonPublic)!
                    .MakeGenericMethod(elementType);

                return (Func<IEnumerable, bool>)m.CreateDelegate(typeof(Func<IEnumerable, bool>));
            });

        public static Func<IEnumerable, int> GetCount(Type elementType)
            => CountDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(EnumerableDispatch)
                    .GetMethod(nameof(CountImpl), BindingFlags.Static | BindingFlags.NonPublic)!
                    .MakeGenericMethod(elementType);

                return (Func<IEnumerable, int>)m.CreateDelegate(typeof(Func<IEnumerable, int>));
            });

        public static Func<IEnumerable, Delegate, bool> GetAll(Type elementType)
            => AllDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(EnumerableDispatch)
                    .GetMethod(nameof(AllImpl), BindingFlags.Static | BindingFlags.NonPublic)!
                    .MakeGenericMethod(elementType);

                return (Func<IEnumerable, Delegate, bool>)m.CreateDelegate(typeof(Func<IEnumerable, Delegate, bool>));
            });

        private static IEnumerable WhereImpl<T>(IEnumerable source, Delegate predicate)
            => Enumerable.Where((IEnumerable<T>)source, (Func<T, bool>)predicate);

        private static IEnumerable SelectImpl<TSource, TResult>(IEnumerable source, Delegate selector)
            => Enumerable.Select((IEnumerable<TSource>)source, (Func<TSource, TResult>)selector);

        private static IEnumerable TakeImpl<T>(IEnumerable source, int count)
            => Enumerable.Take((IEnumerable<T>)source, count);

        private static bool AnyImpl<T>(IEnumerable source)
            => Enumerable.Any((IEnumerable<T>)source);

        private static int CountImpl<T>(IEnumerable source)
            => Enumerable.Count((IEnumerable<T>)source);

        private static bool AllImpl<T>(IEnumerable source, Delegate predicate)
            => Enumerable.All((IEnumerable<T>)source, (Func<T, bool>)predicate);
    }

    private static bool TryGetEnumerableElementType(Type type, out Type elementType)
    {
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

    private IEnumerable<TEntity> ApplyInclude(IEnumerable<TEntity> source, LambdaExpression navigation)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(navigation);

        var navType = navigation.ReturnType;
        if (navType.IsValueType)
            throw new NotSupportedException("Include only supports reference-type navigations.");

        return IncludeDelegates.GetOrAdd(navType, static navType =>
        {
            var factoryMethod = typeof(Db2QueryProvider<TEntity, TRow>)
                .GetMethod(nameof(ApplyIncludeTyped), BindingFlags.Static | BindingFlags.NonPublic)!;

            var generic = factoryMethod.MakeGenericMethod(navType);
            return (Func<Db2QueryProvider<TEntity, TRow>, IEnumerable<TEntity>, LambdaExpression, IEnumerable<TEntity>>)
                generic.CreateDelegate(typeof(Func<Db2QueryProvider<TEntity, TRow>, IEnumerable<TEntity>, LambdaExpression, IEnumerable<TEntity>>));
        })(this, source, navigation);
    }

    private static IEnumerable<TEntity> ApplyIncludeTyped<TRelated>(
        Db2QueryProvider<TEntity, TRow> provider,
        IEnumerable<TEntity> source,
        LambdaExpression navigation)
        where TRelated : class
        => provider.ApplyIncludeCore(source, (Expression<Func<TEntity, TRelated>>)navigation);

    private IEnumerable<TEntity> ApplyIncludeCore<TRelated>(IEnumerable<TEntity> source, Expression<Func<TEntity, TRelated>> navigation)
        where TRelated : class
    {
        if (navigation.Parameters is not { Count: 1 } || navigation.Parameters[0].Type != typeof(TEntity))
            throw new NotSupportedException("Include navigation must be a lambda with a single parameter matching the entity type.");

        var body = navigation.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            body = u.Operand;

        if (body is not MemberExpression { Member: PropertyInfo or FieldInfo } member)
            throw new NotSupportedException("Include only supports simple member access (e.g., x => x.Parent). ");

        if (member.Expression != navigation.Parameters[0])
            throw new NotSupportedException("Include only supports direct member access on the root entity parameter.");

        var navMember = member.Member;
        var navName = navMember.Name;

        if (!IsWritable(navMember))
            throw new NotSupportedException($"Navigation member '{navName}' must be writable.");

        if (_model.TryGetReferenceNavigation(typeof(TEntity), navMember, out var modelNav))
            return ApplyReferenceInclude(source, navMember, navName, modelNav);

        if (_model.TryGetCollectionNavigation(typeof(TEntity), navMember, out var collectionNav))
            return ApplyCollectionInclude(source, navMember, navName, collectionNav);

        throw new NotSupportedException($"Include navigation '{typeof(TEntity).FullName}.{navName}' is not configured. Configure the navigation in OnModelCreating, or ensure schema FK conventions can apply.");

        IEnumerable<TEntity> ApplyReferenceInclude(IEnumerable<TEntity> referenceSource, MemberInfo referenceNavMember, string referenceNavName, Db2ReferenceNavigation referenceNav)
        {
            if (!IsReadable(referenceNav.SourceKeyMember))
                throw new NotSupportedException($"Navigation key member '{referenceNav.SourceKeyMember.Name}' must be readable.");

            if (referenceNav.Kind != Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey &&
                referenceNav.Kind != Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne)
            {
                throw new NotSupportedException($"Include navigation '{typeof(TEntity).FullName}.{referenceNavName}' has unsupported kind '{referenceNav.Kind}'.");
            }

            var keyGetter = CreateIntGetter(referenceNav.SourceKeyMember);
            var navigationSetter = CreateNavigationSetter<TRelated>(referenceNavMember);

            var targetEntityType = _model.GetEntityType(referenceNav.TargetClrType);
            var (relatedFile, relatedSchema) = _tableResolver(targetEntityType.TableName);
            var relatedMaterializer = new Db2EntityMaterializer<TRelated, TRow>(relatedSchema);

            var entitiesWithKeys = new List<(TEntity Entity, int Key)>();
            HashSet<int> keys = [];

            foreach (var entity in referenceSource)
            {
                var key = keyGetter(entity);
                entitiesWithKeys.Add((entity, key));
                if (key != 0)
                    keys.Add(key);
            }

            Dictionary<int, TRelated> relatedByKey = new(capacity: Math.Min(keys.Count, relatedFile.RecordsCount));
            if (keys is { Count: not 0 })
            {
                foreach (var row in relatedFile.EnumerateRows())
                {
                    var rowId = row.Get<int>(Db2VirtualFieldIndex.Id);
                    if (!keys.Contains(rowId))
                        continue;

                    if (!relatedFile.TryGetRowHandle(rowId, out var handle))
                        continue;

                    relatedByKey[rowId] = relatedMaterializer.Materialize(relatedFile, handle);
                }
            }

            foreach (var (entity, key) in entitiesWithKeys)
            {
                relatedByKey.TryGetValue(key, out var related);
                navigationSetter(entity, related);
                yield return entity;
            }
        }

        IEnumerable<TEntity> ApplyCollectionInclude(IEnumerable<TEntity> collectionSource, MemberInfo collectionNavMember, string collectionNavName, Db2CollectionNavigation collectionNav)
        {
            if (collectionNav is { Kind: Db2CollectionNavigationKind.ForeignKeyArrayToPrimaryKey })
            {
                return ApplyCollectionIncludeViaDelegate(
                    ForeignKeyArrayToPrimaryKeyIncludeDelegates,
                    nameof(ApplyForeignKeyArrayToPrimaryKeyInclude),
                    collectionNav,
                    collectionNavName,
                    collectionSource,
                    collectionNavMember);
            }

            if (collectionNav is { Kind: Db2CollectionNavigationKind.DependentForeignKeyToPrimaryKey })
            {
                return ApplyCollectionIncludeViaDelegate(
                    DependentForeignKeyToPrimaryKeyIncludeDelegates,
                    nameof(ApplyDependentForeignKeyToPrimaryKeyInclude),
                    collectionNav,
                    collectionNavName,
                    collectionSource,
                    collectionNavMember);
            }

            throw new NotSupportedException($"Include navigation '{typeof(TEntity).FullName}.{collectionNavName}' has unsupported kind '{collectionNav.Kind}'.");

            IEnumerable<TEntity> ApplyCollectionIncludeViaDelegate(
                ConcurrentDictionary<(Type NavigationType, Type TargetType), Func<Db2QueryProvider<TEntity, TRow>, IEnumerable<TEntity>, MemberInfo, string, Db2CollectionNavigation, IEnumerable<TEntity>>> cache,
                string methodName,
                Db2CollectionNavigation nav,
                string navName,
                IEnumerable<TEntity> src,
                MemberInfo member)
            {
                if (!nav.TargetClrType.IsClass)
                    throw new NotSupportedException($"Collection navigation '{typeof(TEntity).FullName}.{navName}' target type must be a reference type.");

                var key = (NavigationType: typeof(TRelated), TargetType: nav.TargetClrType);
                var d = cache.GetOrAdd(key, _ =>
                {
                    var m = typeof(Db2QueryProvider<TEntity, TRow>)
                        .GetMethod(methodName, BindingFlags.Static | BindingFlags.NonPublic)!;

                    var g = m.MakeGenericMethod(typeof(TRelated), nav.TargetClrType);
                    return (Func<Db2QueryProvider<TEntity, TRow>, IEnumerable<TEntity>, MemberInfo, string, Db2CollectionNavigation, IEnumerable<TEntity>>)
                        g.CreateDelegate(typeof(Func<Db2QueryProvider<TEntity, TRow>, IEnumerable<TEntity>, MemberInfo, string, Db2CollectionNavigation, IEnumerable<TEntity>>));
                });

                return d(this, src, member, navName, nav);
            }
        }

    }

    private static IEnumerable<TEntity> ApplyForeignKeyArrayToPrimaryKeyInclude<TRelatedNav, TTarget>(
        Db2QueryProvider<TEntity, TRow> provider,
        IEnumerable<TEntity> source,
        MemberInfo navMember,
        string navName,
        Db2CollectionNavigation nav)
        where TRelatedNav : class
        where TTarget : class
    {
        if (nav.SourceKeyCollectionMember is null || nav.SourceKeyFieldSchema is null)
            throw new NotSupportedException($"Collection navigation '{typeof(TEntity).FullName}.{navName}' must specify a source key collection member.");

        if (!IsReadable(nav.SourceKeyCollectionMember))
            throw new NotSupportedException($"Navigation key member '{nav.SourceKeyCollectionMember.Name}' must be readable.");

        if (navMember.GetMemberType() != typeof(TRelatedNav))
            throw new NotSupportedException($"Collection navigation '{typeof(TEntity).FullName}.{navName}' must have CLR type '{typeof(TRelatedNav).FullName}'.");

        if (!typeof(TRelatedNav).IsAssignableFrom(typeof(TTarget[])))
            throw new NotSupportedException($"Collection navigation '{typeof(TEntity).FullName}.{navName}' must be assignable from '{typeof(TTarget[]).FullName}'.");

        var keyListGetter = CreateIntEnumerableGetter(nav.SourceKeyCollectionMember);
        var navigationSetter = CreateNavigationSetter<TRelatedNav>(navMember);

        var targetEntityType = provider._model.GetEntityType(typeof(TTarget));
        var (relatedFile, relatedSchema) = provider._tableResolver(targetEntityType.TableName);
        var relatedMaterializer = new Db2EntityMaterializer<TTarget, TRow>(relatedSchema);

        var entitiesWithIds = new List<(TEntity Entity, int[] Ids)>();
        HashSet<int> keys = [];

        foreach (var entity in source)
        {
            var idsEnumerable = keyListGetter(entity);
            var ids = idsEnumerable as int[] ?? idsEnumerable?.ToArray() ?? [];

            entitiesWithIds.Add((entity, ids));
            for (var i = 0; i < ids.Length; i++)
            {
                var id = ids[i];
                if (id != 0)
                    keys.Add(id);
            }
        }

        Dictionary<int, TTarget> relatedByKey = new(capacity: Math.Min(keys.Count, relatedFile.RecordsCount));
        if (keys is { Count: not 0 })
        {
            foreach (var row in relatedFile.EnumerateRows())
            {
                var rowId = row.Get<int>(Db2VirtualFieldIndex.Id);
                if (!keys.Contains(rowId))
                    continue;

                if (!relatedFile.TryGetRowHandle(rowId, out var handle))
                    continue;

                relatedByKey[rowId] = relatedMaterializer.Materialize(relatedFile, handle);
            }
        }

        foreach (var (entity, ids) in entitiesWithIds)
        {
            var relatedArray = new TTarget[ids.Length];
            for (var i = 0; i < ids.Length; i++)
            {
                var id = ids[i];
                if (id == 0)
                    continue;

                if (relatedByKey.TryGetValue(id, out var related))
                    relatedArray[i] = related;
            }

            var value = (TRelatedNav)(IEnumerable<TTarget>)relatedArray;
            navigationSetter(entity, value);
            yield return entity;
        }
    }

    private static IEnumerable<TEntity> ApplyDependentForeignKeyToPrimaryKeyInclude<TRelatedNav, TTarget>(
        Db2QueryProvider<TEntity, TRow> provider,
        IEnumerable<TEntity> source,
        MemberInfo navMember,
        string navName,
        Db2CollectionNavigation nav)
        where TRelatedNav : class
        where TTarget : class
    {
        if (nav.PrincipalKeyMember is null)
            throw new NotSupportedException($"Collection navigation '{typeof(TEntity).FullName}.{navName}' must specify a principal key member.");

        if (nav.DependentForeignKeyFieldSchema is null || nav.DependentForeignKeyMember is null)
            throw new NotSupportedException($"Collection navigation '{typeof(TEntity).FullName}.{navName}' must specify a dependent foreign key member.");

        if (!IsReadable(nav.PrincipalKeyMember))
            throw new NotSupportedException($"Principal key member '{nav.PrincipalKeyMember.Name}' must be readable.");

        if (navMember.GetMemberType() != typeof(TRelatedNav))
            throw new NotSupportedException($"Collection navigation '{typeof(TEntity).FullName}.{navName}' must have CLR type '{typeof(TRelatedNav).FullName}'.");

        if (!typeof(TRelatedNav).IsAssignableFrom(typeof(TTarget[])))
            throw new NotSupportedException($"Collection navigation '{typeof(TEntity).FullName}.{navName}' must be assignable from '{typeof(TTarget[]).FullName}'.");

        var navigationSetter = CreateNavigationSetter<TRelatedNav>(navMember);
        var principalKeyGetter = CreateIntGetter(nav.PrincipalKeyMember);

        var entitiesWithKeys = new List<(TEntity Entity, int Key)>();
        HashSet<int> keys = [];
        foreach (var entity in source)
        {
            var key = principalKeyGetter(entity);
            entitiesWithKeys.Add((entity, key));
            if (key != 0)
                keys.Add(key);
        }

        var targetEntityType = provider._model.GetEntityType(typeof(TTarget));
        var (relatedFile, relatedSchema) = provider._tableResolver(targetEntityType.TableName);
        var relatedMaterializer = new Db2EntityMaterializer<TTarget, TRow>(relatedSchema);

        Dictionary<int, List<TTarget>> dependentsByKey = [];
        if (keys is { Count: not 0 })
        {
            var fkFieldIndex = nav.DependentForeignKeyFieldSchema.Value.ColumnStartIndex;
            foreach (var row in relatedFile.EnumerateRows())
            {
                var fk = row.Get<int>(fkFieldIndex);
                if (fk == 0 || !keys.Contains(fk))
                    continue;

                var rowId = row.Get<int>(Db2VirtualFieldIndex.Id);
                if (!relatedFile.TryGetRowHandle(rowId, out var handle))
                    continue;

                var dependent = relatedMaterializer.Materialize(relatedFile, handle);
                if (!dependentsByKey.TryGetValue(fk, out var list))
                {
                    list = [];
                    dependentsByKey.Add(fk, list);
                }

                list.Add(dependent);
            }
        }

        foreach (var (entity, key) in entitiesWithKeys)
        {
            if (!dependentsByKey.TryGetValue(key, out var dependents) || dependents.Count == 0)
            {
                var empty = Array.Empty<TTarget>();
                var emptyValue = (TRelatedNav)(IEnumerable<TTarget>)empty;
                navigationSetter(entity, emptyValue);
                yield return entity;
                continue;
            }

            var array = new TTarget[dependents.Count];
            for (var i = 0; i < dependents.Count; i++)
                array[i] = dependents[i];

            var value = (TRelatedNav)(IEnumerable<TTarget>)array;
            navigationSetter(entity, value);
            yield return entity;
        }
    }

    private static Func<TEntity, IEnumerable<int>?> CreateIntEnumerableGetter(MemberInfo member)
    {
        var entity = Expression.Parameter(typeof(TEntity), "entity");

        Expression access = member switch
        {
            PropertyInfo p => Expression.Property(entity, p),
            FieldInfo f => Expression.Field(entity, f),
            _ => throw new InvalidOperationException($"Unexpected member type: {member.GetType().FullName}"),
        };

        var memberType = member.GetMemberType();

        if (memberType == typeof(int[]))
            return Expression.Lambda<Func<TEntity, IEnumerable<int>?>>(Expression.Convert(access, typeof(IEnumerable<int>)), entity).Compile();

        if (typeof(IEnumerable<int>).IsAssignableFrom(memberType))
            return Expression.Lambda<Func<TEntity, IEnumerable<int>?>>(Expression.Convert(access, typeof(IEnumerable<int>)), entity).Compile();

        throw new NotSupportedException($"Expected an int enumerable member but found '{memberType.FullName}'.");
    }


    private static Func<TEntity, int> CreateIntGetter(MemberInfo member)
    {
        var entity = Expression.Parameter(typeof(TEntity), "entity");

        Expression access = member switch
        {
            PropertyInfo p => Expression.Property(entity, p),
            FieldInfo f => Expression.Field(entity, f),
            _ => throw new InvalidOperationException($"Unexpected member type: {member.GetType().FullName}"),
        };

        var memberType = member.GetMemberType();
        access = ConvertToInt32NoBox(access, memberType);
        return Expression.Lambda<Func<TEntity, int>>(access, entity).Compile();
    }

    private static Expression ConvertToInt32NoBox(Expression value, Type valueType)
    {
        if (Nullable.GetUnderlyingType(valueType) is { } nullableUnderlying)
        {
            var getValueOrDefault = valueType.GetMethod(nameof(Nullable<int>.GetValueOrDefault), Type.EmptyTypes)!;
            value = Expression.Call(value, getValueOrDefault);
            valueType = nullableUnderlying;
        }

        if (valueType.IsEnum)
        {
            var underlying = Enum.GetUnderlyingType(valueType);
            value = Expression.Convert(value, underlying);
            valueType = underlying;
        }

        return valueType == typeof(int)
            ? value
            : Expression.Convert(value, typeof(int));
    }

    private static Action<TEntity, TRelated?> CreateNavigationSetter<TRelated>(MemberInfo navMember)
        where TRelated : class
    {
        var entity = Expression.Parameter(typeof(TEntity), "entity");
        var value = Expression.Parameter(typeof(TRelated), "value");

        Expression memberAccess = navMember switch
        {
            PropertyInfo p => Expression.Property(entity, p),
            FieldInfo f => Expression.Field(entity, f),
            _ => throw new InvalidOperationException($"Unexpected navigation member type: {navMember.GetType().FullName}"),
        };

        var assign = Expression.Assign(memberAccess, value);
        return Expression.Lambda<Action<TEntity, TRelated?>>(assign, entity, value).Compile();
    }

    private static bool IsReadable(MemberInfo member)
        => member switch
        {
            PropertyInfo p => p.GetMethod is not null,
            FieldInfo => true,
            _ => false,
        };

    private static bool IsWritable(MemberInfo member)
        => member switch
        {
            PropertyInfo p => p.SetMethod is not null,
            FieldInfo f => !f.IsInitOnly,
            _ => false,
        };
}
