using MimironSQL.Db2;
using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Model;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Formats;

using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using MimironSQL.Extensions;

namespace MimironSQL.Db2.Query;

internal sealed class Db2QueryProvider<TEntity>(
    Wdc5File file,
    Db2TableSchema schema,
    Db2Model model,
    Func<string, (IDb2File File, Db2TableSchema Schema)> tableResolver) : IQueryProvider
{
    private readonly Db2EntityMaterializer<TEntity> _materializer = new(schema);
    private readonly Db2Model _model = model;
    private readonly Func<string, (IDb2File File, Db2TableSchema Schema)> _tableResolver = tableResolver;

    internal TEntity Materialize(Wdc5Row row) => _materializer.Materialize(row);

    public IQueryable CreateQuery(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        var elementType = expression.Type.GetGenericArguments().FirstOrDefault() ?? typeof(object);
        var queryableType = typeof(Db2Queryable<>).MakeGenericType(elementType);
        return (IQueryable)Activator.CreateInstance(queryableType, this, expression)!;
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        return new Db2Queryable<TElement>(this, expression);
    }

    public object? Execute(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        var resultType = expression.Type;

        if (TryGetEnumerableElementType(resultType, out var elementType))
        {
            var method = typeof(Db2QueryProvider<TEntity>).GetMethod(nameof(ExecuteEnumerable), BindingFlags.Instance | BindingFlags.NonPublic)!;
            var generic = method.MakeGenericMethod(elementType);
            return generic.Invoke(this, [expression]);
        }

        var scalarMethod = typeof(Db2QueryProvider<TEntity>).GetMethod(nameof(ExecuteScalar), BindingFlags.Instance | BindingFlags.NonPublic)!;
        var scalarGeneric = scalarMethod.MakeGenericMethod(resultType);
        return scalarGeneric.Invoke(this, [expression]);
    }

    public TResult Execute<TResult>(Expression expression)
        => (TResult)Execute(expression)!;

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
                if (TryExecuteEnumerablePruned(preEntityWhere, stopAfter, selectOp.Selector, postOps, out var pruned))
                    return (IEnumerable<TElement>)pruned;
            }

            var canAttemptNavigationPrune =
                selectOp.Selector is { Parameters.Count: 1 } &&
                selectOp.Selector.Parameters[0].Type == typeof(TEntity) &&
                selectOp.Selector.ReturnType == typeof(TElement) &&
                SelectorUsesNavigation(selectOp.Selector) &&
                postOps.All(op => op is not Db2WhereOperation && op is not Db2IncludeOperation) &&
                postOps.Count(op => op is Db2SelectOperation) <= 1;

            if (canAttemptNavigationPrune && TryExecuteNavigationProjectionPruned(preEntityWhere, stopAfter, selectOp.Selector, postOps, out var navPruned))
                return (IEnumerable<TElement>)navPruned;
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

    private bool TryExecuteEnumerablePruned(
        IList<Expression<Func<TEntity, bool>>> preEntityWhere,
        int? stopAfter,
        LambdaExpression selector,
        List<Db2QueryOperation> postOps,
        out IEnumerable result)
    {
        result = Array.Empty<object>();

        if (selector is not Expression<Func<TEntity, TEntity>> && selector.Parameters is not { Count: 1 })
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

        var selectorReturnType = selector.ReturnType;
        var projectorMethod = typeof(Db2QueryProvider<TEntity>).GetMethod(nameof(TryCreateProjector), BindingFlags.Instance | BindingFlags.NonPublic)!;
        var projectorGeneric = projectorMethod.MakeGenericMethod(selectorReturnType);
        if (projectorGeneric.Invoke(this, [selector]) is not ValueTuple<Delegate, Db2SourceRequirements> compiled)
            return false;

        requirements.Columns.UnionWith(compiled.Item2.Columns);

        // Pruning is only safe when we can satisfy the projection/predicate from row-level reads.
        // Virtual strings cannot be materialized from WDC5 rows.
        if (requirements.Columns.Any(c => c is { Kind: Db2RequiredColumnKind.String, Field.IsVirtual: true }))
            return false;

        var enumerateMethod = typeof(Db2QueryProvider<TEntity>).GetMethod(nameof(EnumerateProjected), BindingFlags.Instance | BindingFlags.NonPublic)!;
        var enumerateGeneric = enumerateMethod.MakeGenericMethod(selectorReturnType);
        var projected = (IEnumerable)enumerateGeneric.Invoke(this, [rowPredicates, compiled.Item1, stopAfter])!;

        IEnumerable current = projected;
        var currentElementType = selectorReturnType;

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

        result = current;
        return true;
    }

    private bool TryExecuteNavigationProjectionPruned(
        IList<Expression<Func<TEntity, bool>>> preEntityWhere,
        int? stopAfter,
        LambdaExpression selector,
        List<Db2QueryOperation> postOps,
        out IEnumerable result)
    {
        result = Array.Empty<object>();

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

        var selectorReturnType = selector.ReturnType;
        var projectMethod = typeof(Db2QueryProvider<TEntity>).GetMethod(nameof(EnumerateNavigationProjected), BindingFlags.Instance | BindingFlags.NonPublic)!;
        var projectGeneric = projectMethod.MakeGenericMethod(selectorReturnType);
        var projected = (IEnumerable)projectGeneric.Invoke(this, [rowPredicates, stopAfter, navAccesses, selector])!;

        IEnumerable current = projected;
        var currentElementType = selectorReturnType;

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

        result = current;
        return true;
    }

    private (Delegate Projector, Db2SourceRequirements Requirements)? TryCreateProjector<TProjected>(LambdaExpression selector)
    {
        if (selector is not Expression<Func<TEntity, TProjected>> typed)
            return null;

        return Db2RowProjectorCompiler.TryCompile(schema, typed, out var projector, out var requirements)
            ? (projector, requirements)
            : null;
    }

    private IEnumerable<TProjected> EnumerateProjected<TProjected>(
        List<Func<Wdc5Row, bool>> rowPredicates,
        Delegate projector,
        int? take)
    {
        var typedProjector = (Func<Wdc5Row, TProjected>)projector;

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

            yield return typedProjector(row);

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
        if (!TryGetEnumerableElementType(pipeline.ExpressionWithoutFinalOperator.Type, out var sequenceElementType))
            throw new NotSupportedException("Unable to determine sequence element type for scalar execution.");

        var enumerable = Execute(expressionWithFinalStripped: pipeline.ExpressionWithoutFinalOperator, sequenceElementType);

        if (pipeline is { FinalOperator: Db2FinalOperator.All })
        {
            if (pipeline.FinalPredicate is null)
                throw new NotSupportedException("Queryable.All requires a predicate for this provider.");

            var typedPredicate = pipeline.FinalPredicate.Compile();

            var all = typeof(Enumerable)
                .GetMethods(BindingFlags.Public | BindingFlags.Static)
                .Single(m => m.Name == nameof(Enumerable.All) && m.GetParameters() is { Length: 2 })
                .MakeGenericMethod(sequenceElementType);

            return (TResult)all.Invoke(null, [enumerable, typedPredicate])!;
        }

        var methodName = pipeline.FinalOperator switch
        {
            Db2FinalOperator.First => nameof(Enumerable.First),
            Db2FinalOperator.FirstOrDefault => nameof(Enumerable.FirstOrDefault),
            Db2FinalOperator.Any => nameof(Enumerable.Any),
            Db2FinalOperator.Count => nameof(Enumerable.Count),
            Db2FinalOperator.Single => nameof(Enumerable.Single),
            Db2FinalOperator.SingleOrDefault => nameof(Enumerable.SingleOrDefault),
            _ => throw new NotSupportedException($"Unsupported scalar operator: {pipeline.FinalOperator}."),
        };

        var scalar = typeof(Enumerable)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == methodName && m.GetParameters() is { Length: 1 })
            .MakeGenericMethod(sequenceElementType);

        return (TResult)scalar.Invoke(null, [enumerable])!;
    }

    private object Execute(Expression expressionWithFinalStripped, Type elementType)
    {
        var exec = typeof(Db2QueryProvider<TEntity>).GetMethod(nameof(ExecuteEnumerable), BindingFlags.Instance | BindingFlags.NonPublic)!;
        var generic = exec.MakeGenericMethod(elementType);
        return generic.Invoke(this, [expressionWithFinalStripped])!;
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

        var where = typeof(Enumerable)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m =>
                m.Name == nameof(Enumerable.Where) &&
                m.GetParameters() is { Length: 2 } &&
                m.GetParameters()[1].ParameterType.IsGenericType &&
                m.GetParameters()[1].ParameterType.GetGenericArguments() is { Length: 2 })
            .MakeGenericMethod(sourceElementType);

        return (IEnumerable)where.Invoke(null, [source, typedPredicate])!;
    }

    private static IEnumerable ApplySelect(IEnumerable source, Type sourceElementType, LambdaExpression selector)
    {
        var resultType = selector.ReturnType;
        var typedSelector = selector.Compile();

        var select = typeof(Enumerable)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m =>
                m.Name == nameof(Enumerable.Select) &&
                m.GetParameters() is { Length: 2 } &&
                m.GetParameters()[1].ParameterType.IsGenericType &&
                m.GetParameters()[1].ParameterType.GetGenericArguments() is { Length: 2 })
            .MakeGenericMethod(sourceElementType, resultType);

        return (IEnumerable)select.Invoke(null, [source, typedSelector])!;
    }

    private static IEnumerable ApplyTake(IEnumerable source, Type sourceElementType, int count)
    {
        var take = typeof(Enumerable)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m =>
                m.Name == nameof(Enumerable.Take) &&
                m.GetParameters() is { Length: 2 } &&
                m.GetParameters()[1].ParameterType == typeof(int))
            .MakeGenericMethod(sourceElementType);

        return (IEnumerable)take.Invoke(null, [source, count])!;
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
        var plan = new IncludePlan(navigation, _model, _tableResolver);

        var entitiesWithKeys = new List<(TEntity Entity, int Key)>();
        HashSet<int> keys = [];

        foreach (var entity in source)
        {
            var key = plan.Key(entity);
            entitiesWithKeys.Add((entity, key));
            if (key != 0)
                keys.Add(key);
        }

        var relatedByKey = plan.LoadEntities(keys);

        foreach (var (entity, key) in entitiesWithKeys)
        {
            relatedByKey.TryGetValue(key, out var related);
            plan.Set(entity, related);
            yield return entity;
        }
    }

    private sealed class IncludePlan
    {
        private readonly Func<TEntity, int> keyGetter;
        private readonly Action<TEntity, object?> navigationSetter;
        private readonly Func<HashSet<int>, Dictionary<int, object?>> loadEntities;

        public IncludePlan(
            LambdaExpression navigation,
            Db2Model model,
            Func<string, (IDb2File File, Db2TableSchema Schema)> tableResolver)
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
            var navType = navMember.GetMemberType();

            if (!IsWritable(navMember))
                throw new NotSupportedException($"Navigation member '{navName}' must be writable.");

            if (!model.TryGetReferenceNavigation(typeof(TEntity), navMember, out var modelNav))
                throw new NotSupportedException($"Include navigation '{typeof(TEntity).FullName}.{navName}' is not configured. Configure the navigation in OnModelCreating, or ensure schema FK conventions can apply.");

            navigationSetter = CreateNavigationSetter(navMember, navType);

            if (!IsReadable(modelNav.SourceKeyMember))
                throw new NotSupportedException($"Navigation key member '{modelNav.SourceKeyMember.Name}' must be readable.");

            var targetEntityType = model.GetEntityType(modelNav.TargetClrType);
            loadEntities = CreateEntitiesLoader(navType, targetEntityType.TableName, tableResolver);

            keyGetter = CreateIntGetter(modelNav.SourceKeyMember);

            if (modelNav.Kind != Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey &&
                modelNav.Kind != Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne)
            {
                throw new NotSupportedException($"Include navigation '{typeof(TEntity).FullName}.{navName}' has unsupported kind '{modelNav.Kind}'.");
            }
        }

        public int Key(TEntity entity) => keyGetter(entity);

        public void Set(TEntity entity, object? related)
            => navigationSetter(entity, related);

        public Dictionary<int, object?> LoadEntities(HashSet<int> keys)
            => loadEntities(keys);

        private static Func<TEntity, int> CreateIntGetter(MemberInfo member)
        {
            var entity = Expression.Parameter(typeof(TEntity), "entity");

            Expression access = member switch
            {
                PropertyInfo p => Expression.Property(entity, p),
                FieldInfo f => Expression.Field(entity, f),
                _ => throw new InvalidOperationException($"Unexpected member type: {member.GetType().FullName}"),
            };

            var toInt32 = typeof(Convert).GetMethod(nameof(Convert.ToInt32), [typeof(object)])!;
            var call = Expression.Call(toInt32, Expression.Convert(access, typeof(object)));
            return Expression.Lambda<Func<TEntity, int>>(call, entity).Compile();
        }

        private static Action<TEntity, object?> CreateNavigationSetter(MemberInfo navMember, Type navType)
        {
            var entity = Expression.Parameter(typeof(TEntity), "entity");
            var value = Expression.Parameter(typeof(object), "value");

            Expression memberAccess = navMember switch
            {
                PropertyInfo p => Expression.Property(entity, p),
                FieldInfo f => Expression.Field(entity, f),
                _ => throw new InvalidOperationException($"Unexpected navigation member type: {navMember.GetType().FullName}"),
            };

            var assign = Expression.Assign(memberAccess, Expression.Convert(value, navType));
            return Expression.Lambda<Action<TEntity, object?>>(assign, entity, value).Compile();
        }

        private static Func<HashSet<int>, Dictionary<int, object?>> CreateEntitiesLoader(
            Type entityType,
            string referencedTableName,
            Func<string, (IDb2File File, Db2TableSchema Schema)> tableResolver)
        {
            var (fileHandle, schema) = tableResolver(referencedTableName);
            var file = (Wdc5File)fileHandle;
            var materializeRow = CreateRowMaterializer(entityType, schema);

            return keys =>
            {
                if (keys is { Count: 0 })
                    return new Dictionary<int, object?>();

                Dictionary<int, object?> relatedByKey = new(capacity: Math.Min(keys.Count, file.Header.RecordsCount));

                foreach (var row in file.EnumerateRows().Where(row => keys.Contains(row.Id)))
                {
                    relatedByKey[row.Id] = materializeRow(row);
                }

                return relatedByKey;
            };
        }

        private static Func<Wdc5Row, object?> CreateRowMaterializer(Type entityType, Db2TableSchema schema)
        {
            var materializerType = typeof(Db2EntityMaterializer<>).MakeGenericType(entityType);
            var materializer = Activator.CreateInstance(materializerType, schema)!;

            var materialize = materializerType.GetMethod(nameof(Db2EntityMaterializer<>.Materialize), BindingFlags.Instance | BindingFlags.Public)!;

            var row = Expression.Parameter(typeof(Wdc5Row), "row");
            var call = Expression.Call(Expression.Constant(materializer), materialize, row);
            var boxed = Expression.Convert(call, typeof(object));
            return Expression.Lambda<Func<Wdc5Row, object?>>(boxed, row).Compile();
        }

        private static MemberInfo? FindMember(Type type, string name)
        {
            var members = type.GetMember(name, BindingFlags.Instance | BindingFlags.Public);
            foreach (var m in members)
            {
                if (m is PropertyInfo or FieldInfo)
                    return m;
            }

            return null;
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
}
