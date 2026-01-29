using MimironSQL.Db2;
using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Model;
using MimironSQL.Db2.Wdc5;

using System.Collections.Concurrent;
using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using MimironSQL.Extensions;

namespace MimironSQL.Db2.Query;

internal sealed class Db2QueryProvider<TEntity>(
    Wdc5File file,
    Db2TableSchema schema,
    Db2Model model,
    Func<string, (Wdc5File File, Db2TableSchema Schema)> tableResolver) : IQueryProvider
{
    private static readonly ConcurrentDictionary<Type, Func<Db2QueryProvider<TEntity>, Expression, object>> ExecuteEnumerableDelegates = new();
    private static readonly ConcurrentDictionary<Type, Func<Db2QueryProvider<TEntity>, IEnumerable<TEntity>, LambdaExpression, IEnumerable<TEntity>>> IncludeDelegates = new();

    private readonly Db2EntityMaterializer<TEntity> _materializer = new(schema);
    private readonly Db2Model _model = model;
    private readonly Func<string, (Wdc5File File, Db2TableSchema Schema)> _tableResolver = tableResolver;

    internal TEntity Materialize(Wdc5Row row) => _materializer.Materialize(row);

    public IQueryable CreateQuery(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        var elementType = expression.Type.GetGenericArguments().FirstOrDefault() ?? typeof(object);

        // Non-generic CreateQuery is not used by the normal strongly-typed Queryable surface.
        // Keep it functional without using reflection invocation.
        var factoryMethod = typeof(Db2QueryProvider<TEntity>)
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

        // Non-generic Execute returns object, which necessarily boxes scalar value types.
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

    private static object ExecuteEnumerableForResult<TElement>(Db2QueryProvider<TEntity> provider, Expression expression)
        => provider.ExecuteEnumerable<TElement>(expression);

    private static Func<Db2QueryProvider<TEntity>, Expression, object> GetExecuteEnumerableDelegate(Type elementType)
        => ExecuteEnumerableDelegates.GetOrAdd(elementType, static elementType =>
        {
            var method = typeof(Db2QueryProvider<TEntity>).GetMethod(
                nameof(ExecuteEnumerableForResult),
                BindingFlags.Static | BindingFlags.NonPublic)!
                .MakeGenericMethod(elementType);

            return (Func<Db2QueryProvider<TEntity>, Expression, object>)method.CreateDelegate(
                typeof(Func<Db2QueryProvider<TEntity>, Expression, object>));
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

        var rowPredicates = new List<Func<Wdc5Row, bool>>();
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
        // Virtual strings cannot be materialized from WDC5 rows.
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

        var rowPredicates = new List<Func<Wdc5Row, bool>>();
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

    private (Func<Wdc5Row, TProjected> Projector, Db2SourceRequirements Requirements)? TryCreateProjector<TProjected>(LambdaExpression selector)
    {
        if (selector is not Expression<Func<TEntity, TProjected>> typed)
            return null;

        return Db2RowProjectorCompiler.TryCompile(schema, typed, out var projector, out var requirements)
            ? (projector, requirements)
            : null;
    }

    private IEnumerable<TProjected> EnumerateProjected<TProjected>(
        List<Func<Wdc5Row, bool>> rowPredicates,
        Func<Wdc5Row, TProjected> projector,
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
        List<Func<Wdc5Row, bool>> rowPredicates,
        int? take,
        IReadOnlyList<Db2NavigationMemberAccessPlan> navAccesses,
        LambdaExpression selector)
    {
        IEnumerable<Wdc5Row> FilteredRows()
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

        return Db2NavigationRowProjector.ProjectFromRows<TProjected>(
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
        var rowPredicates = new List<Func<Wdc5Row, bool>>();
        var entityPredicates = new List<Func<TEntity, bool>>();

        foreach (var predicate in predicates)
        {
            if (Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(_model, file, schema, _tableResolver, predicate, out var navPredicate))
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

            var entity = _materializer.Materialize(row);

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
        private static readonly ConcurrentDictionary<Type, Func<object, object, object>> WhereDelegates = new();
        private static readonly ConcurrentDictionary<(Type Source, Type Result), Func<object, object, object>> SelectDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<object, int, object>> TakeDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<object, bool>> AnyDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<object, int>> CountDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<object, object, bool>> AllDelegates = new();

        public static Func<object, object, object> GetWhere(Type elementType)
            => WhereDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(EnumerableDispatch)
                    .GetMethod(nameof(WhereImpl), BindingFlags.Static | BindingFlags.NonPublic)!
                    .MakeGenericMethod(elementType);

                return (Func<object, object, object>)m.CreateDelegate(typeof(Func<object, object, object>));
            });

        public static Func<object, object, object> GetSelect(Type sourceType, Type resultType)
            => SelectDelegates.GetOrAdd((sourceType, resultType), static key =>
            {
                var m = typeof(EnumerableDispatch)
                    .GetMethod(nameof(SelectImpl), BindingFlags.Static | BindingFlags.NonPublic)!
                    .MakeGenericMethod(key.Source, key.Result);

                return (Func<object, object, object>)m.CreateDelegate(typeof(Func<object, object, object>));
            });

        public static Func<object, int, object> GetTake(Type elementType)
            => TakeDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(EnumerableDispatch)
                    .GetMethod(nameof(TakeImpl), BindingFlags.Static | BindingFlags.NonPublic)!
                    .MakeGenericMethod(elementType);

                return (Func<object, int, object>)m.CreateDelegate(typeof(Func<object, int, object>));
            });

        public static Func<object, bool> GetAny(Type elementType)
            => AnyDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(EnumerableDispatch)
                    .GetMethod(nameof(AnyImpl), BindingFlags.Static | BindingFlags.NonPublic)!
                    .MakeGenericMethod(elementType);

                return (Func<object, bool>)m.CreateDelegate(typeof(Func<object, bool>));
            });

        public static Func<object, int> GetCount(Type elementType)
            => CountDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(EnumerableDispatch)
                    .GetMethod(nameof(CountImpl), BindingFlags.Static | BindingFlags.NonPublic)!
                    .MakeGenericMethod(elementType);

                return (Func<object, int>)m.CreateDelegate(typeof(Func<object, int>));
            });

        public static Func<object, object, bool> GetAll(Type elementType)
            => AllDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(EnumerableDispatch)
                    .GetMethod(nameof(AllImpl), BindingFlags.Static | BindingFlags.NonPublic)!
                    .MakeGenericMethod(elementType);

                return (Func<object, object, bool>)m.CreateDelegate(typeof(Func<object, object, bool>));
            });

        private static object WhereImpl<T>(object source, object predicate)
            => Enumerable.Where((IEnumerable<T>)source, (Func<T, bool>)predicate);

        private static object SelectImpl<TSource, TResult>(object source, object selector)
            => Enumerable.Select((IEnumerable<TSource>)source, (Func<TSource, TResult>)selector);

        private static object TakeImpl<T>(object source, int count)
            => Enumerable.Take((IEnumerable<T>)source, count);

        private static bool AnyImpl<T>(object source)
            => Enumerable.Any((IEnumerable<T>)source);

        private static int CountImpl<T>(object source)
            => Enumerable.Count((IEnumerable<T>)source);

        private static bool AllImpl<T>(object source, object predicate)
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

        elementType = typeof(object);
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
            var factoryMethod = typeof(Db2QueryProvider<TEntity>)
                .GetMethod(nameof(ApplyIncludeTyped), BindingFlags.Static | BindingFlags.NonPublic)!;

            var generic = factoryMethod.MakeGenericMethod(navType);
            return (Func<Db2QueryProvider<TEntity>, IEnumerable<TEntity>, LambdaExpression, IEnumerable<TEntity>>)
                generic.CreateDelegate(typeof(Func<Db2QueryProvider<TEntity>, IEnumerable<TEntity>, LambdaExpression, IEnumerable<TEntity>>));
        })(this, source, navigation);
    }

    private static IEnumerable<TEntity> ApplyIncludeTyped<TRelated>(
        Db2QueryProvider<TEntity> provider,
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

        if (!_model.TryGetReferenceNavigation(typeof(TEntity), navMember, out var modelNav))
            throw new NotSupportedException($"Include navigation '{typeof(TEntity).FullName}.{navName}' is not configured. Configure the navigation in OnModelCreating, or ensure schema FK conventions can apply.");

        if (!IsReadable(modelNav.SourceKeyMember))
            throw new NotSupportedException($"Navigation key member '{modelNav.SourceKeyMember.Name}' must be readable.");

        if (modelNav.Kind != Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey &&
            modelNav.Kind != Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne)
        {
            throw new NotSupportedException($"Include navigation '{typeof(TEntity).FullName}.{navName}' has unsupported kind '{modelNav.Kind}'.");
        }

        var keyGetter = CreateIntGetter(modelNav.SourceKeyMember);
        var navigationSetter = CreateNavigationSetter<TRelated>(navMember);

        var targetEntityType = _model.GetEntityType(modelNav.TargetClrType);
        var (relatedFile, relatedSchema) = _tableResolver(targetEntityType.TableName);
        var relatedMaterializer = new Db2EntityMaterializer<TRelated>(relatedSchema);

        var entitiesWithKeys = new List<(TEntity Entity, int Key)>();
        HashSet<int> keys = [];

        foreach (var entity in source)
        {
            var key = keyGetter(entity);
            entitiesWithKeys.Add((entity, key));
            if (key != 0)
                keys.Add(key);
        }

        Dictionary<int, TRelated> relatedByKey = new(capacity: Math.Min(keys.Count, relatedFile.Header.RecordsCount));
        if (keys is { Count: not 0 })
        {
            foreach (var row in relatedFile.EnumerateRows().Where(row => keys.Contains(row.Id)))
            {
                relatedByKey[row.Id] = relatedMaterializer.Materialize(row);
            }
        }

        foreach (var (entity, key) in entitiesWithKeys)
        {
            relatedByKey.TryGetValue(key, out var related);
            navigationSetter(entity, related);
            yield return entity;
        }
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
