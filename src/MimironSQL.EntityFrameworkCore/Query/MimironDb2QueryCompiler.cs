using System.Collections.Concurrent;
using System.Collections;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;

using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;

namespace MimironSQL.EntityFrameworkCore.Query;

internal sealed class MimironDb2QueryExecutor(
    ICurrentDbContext currentDbContext,
    IMimironDb2Store store,
    IDb2ModelBinding db2ModelBinding) : IMimironDb2QueryExecutor
{
    private static readonly ConcurrentDictionary<(Type EntityType, Type RowType, Type ResultType), Func<MimironDb2QueryExecutor, Expression, object?>> ExecuteDelegates = new();

    private readonly DbContext _context = currentDbContext?.Context ?? throw new ArgumentNullException(nameof(currentDbContext));
    private readonly IMimironDb2Store _store = store ?? throw new ArgumentNullException(nameof(store));
    private readonly IDb2ModelBinding _db2ModelBinding = db2ModelBinding ?? throw new ArgumentNullException(nameof(db2ModelBinding));

    public TResult Execute<TResult>(Expression query)
    {
        ArgumentNullException.ThrowIfNull(query);

        query = MimironDb2EfExpressionNormalizer.Normalize(query);

        var rootEntityType = GetRootEntityClrType(query);

        var efEntityType = _context.Model.FindEntityType(rootEntityType)
            ?? throw new NotSupportedException($"Entity type '{rootEntityType.FullName}' is not part of the EF model.");

        // This provider currently uses RowHandle for all shipped formats.
        // Avoid opening/caching full tables just to discover the row type.
        var rowType = typeof(RowHandle);

        var result = ExecuteDelegates.GetOrAdd((rootEntityType, rowType, typeof(TResult)), static key =>
        {
            var method = typeof(MimironDb2QueryExecutor)
                .GetMethod(nameof(ExecuteTyped), BindingFlags.NonPublic | BindingFlags.Instance)!
                .MakeGenericMethod(key.EntityType, key.RowType, key.ResultType);

            return method.CreateDelegate<Func<MimironDb2QueryExecutor, Expression, object?>>();
        })(this, query);

        return (TResult)result!;
    }

    private object? ExecuteTyped<TEntity, TRow, TResult>(Expression query)
        where TEntity : class
        where TRow : struct, IRowHandle
    {
        query = MimironDb2EfExpressionNormalizer.Normalize(query);

        var model = _db2ModelBinding.GetBinding();

        var pipeline = Db2QueryPipeline.Parse(query);

        if (typeof(TRow) == typeof(RowHandle)
            && TryGetPrimaryKeyMemberName(model, typeof(TEntity), out var pkMemberName)
            && TryGetKeyLookupRequest<TEntity>(query, pkMemberName, out var request))
        {
            IDb2EntityFactory keyLookupEntityFactory = new EfLazyLoadingProxyDb2EntityFactory(_context, new ReflectionDb2EntityFactory());

            var efEntityType2 = _context.Model.FindEntityType(typeof(TEntity))
                ?? throw new NotSupportedException($"Entity type '{typeof(TEntity).FullName}' is not part of the EF model.");

            var tableName2 = efEntityType2.GetTableName() ?? typeof(TEntity).Name;

            using var keyLookupSession = new QuerySession<RowHandle>(_context, _store, model);
            keyLookupSession.Warm(query, typeof(TEntity));

            var (rootFile, rootSchema) = keyLookupSession.Resolve(tableName2);

            var rootEntities = MaterializeByIdsFromOpenFile<TEntity>(rootFile, rootSchema, tableName2, request.Ids, request.TakeCount, model, keyLookupEntityFactory);

            IEnumerable current = rootEntities;
            var currentElementType = typeof(TEntity);

            // Apply includes before Select, matching provider semantics.
            for (var i = 0; i < request.Includes.Count; i++)
            {
                current = Db2IncludeChainExecutor.Apply<TEntity, RowHandle>(
                    (IEnumerable<TEntity>)current,
                    model,
                    keyLookupSession.Resolve,
                    request.Includes[i].Members,
                    keyLookupEntityFactory);
            }

            if (request.Select is not null)
            {
                current = ApplySelect(current, currentElementType, request.Select.Selector);
                currentElementType = request.Select.Selector.ReturnType;
            }

            return request.FinalOperator switch
            {
                Db2FinalOperator.None => MaterializeSequenceResult<TResult>(current, currentElementType),
                Db2FinalOperator.First => (TResult)ApplyScalarOperator(Db2FinalOperator.First, current, currentElementType, request.FinalPredicate)!,
                Db2FinalOperator.FirstOrDefault => (TResult?)ApplyScalarOperator(Db2FinalOperator.FirstOrDefault, current, currentElementType, request.FinalPredicate),
                Db2FinalOperator.Single => (TResult)ApplyScalarOperator(Db2FinalOperator.Single, current, currentElementType, request.FinalPredicate)!,
                Db2FinalOperator.SingleOrDefault => (TResult?)ApplyScalarOperator(Db2FinalOperator.SingleOrDefault, current, currentElementType, request.FinalPredicate),
                Db2FinalOperator.Any => (TResult)(object)(bool)ApplyScalarOperator(Db2FinalOperator.Any, current, currentElementType, request.FinalPredicate)!,
                Db2FinalOperator.Count => (TResult)(object)(int)ApplyScalarOperator(Db2FinalOperator.Count, current, currentElementType, request.FinalPredicate)!,
                Db2FinalOperator.All => (TResult)(object)(bool)ApplyScalarOperator(Db2FinalOperator.All, current, currentElementType, request.FinalPredicate)!,
                _ => throw new NotSupportedException($"Unsupported terminal operator for key lookup: {request.FinalOperator}.")
            };
        }

        var efEntityType = _context.Model.FindEntityType(typeof(TEntity))
            ?? throw new NotSupportedException($"Entity type '{typeof(TEntity).FullName}' is not part of the EF model.");

        var tableName = efEntityType.GetTableName() ?? typeof(TEntity).Name;

        // For sequence results, defer opening DB2 files until enumeration and ensure they are disposed
        // when enumeration completes or is aborted.
        if (pipeline.FinalOperator == Db2FinalOperator.None && TryGetEnumerableElementType(typeof(TResult), out var elementType))
        {
            var sequenceInterface = typeof(IEnumerable<>).MakeGenericType(elementType);
            if (typeof(TResult).IsAssignableFrom(sequenceInterface))
            {
                return ExecuteDeferredEnumerable<TEntity, TRow>(query, model, tableName, elementType);
            }
        }

        using var session = new QuerySession<TRow>(_context, _store, model);
        session.Warm(query, typeof(TEntity));

        var (file, schema) = session.Resolve(tableName);

        (IDb2File<TRow> File, Db2TableSchema Schema) TableResolver(string name)
            => session.Resolve(name);

        IDb2EntityFactory queryEntityFactory = new EfLazyLoadingProxyDb2EntityFactory(_context, new ReflectionDb2EntityFactory());
        var provider = new Db2QueryProvider<TEntity, TRow>(file, model, TableResolver, queryEntityFactory);

        var db2EntityType = model.GetEntityType(typeof(TEntity)).WithSchema(tableName, schema);
        _ = db2EntityType;
        var rootQueryable = new Db2Queryable<TEntity>(provider);

        var rewritten = new RootQueryRewriter<TEntity>(rootQueryable).Visit(query);
        return provider.Execute<TResult>(rewritten!);
    }

    private object ExecuteDeferredEnumerable<TEntity, TRow>(Expression query, Db2ModelBinding model, string tableName, Type elementType)
        where TEntity : class
        where TRow : struct, IRowHandle
    {
        var method = typeof(MimironDb2QueryExecutor)
            .GetMethod(nameof(ExecuteDeferredEnumerableTyped), BindingFlags.NonPublic | BindingFlags.Instance)!
            .MakeGenericMethod(typeof(TEntity), typeof(TRow), elementType);

        return method.Invoke(this, [query, model, tableName])!;
    }

    private object ExecuteDeferredEnumerableTyped<TEntity, TRow, TElement>(Expression query, Db2ModelBinding model, string tableName)
        where TEntity : class
        where TRow : struct, IRowHandle
    {
        var enumerable = new DeferredScopeEnumerable<TElement>(() =>
        {
            var session = new QuerySession<TRow>(_context, _store, model);

            // Ensure all required tables are opened once before enumeration begins.
            session.Warm(query, typeof(TEntity));

            var (file, schema) = session.Resolve(tableName);

            (IDb2File<TRow> File, Db2TableSchema Schema) TableResolver(string name)
                => session.Resolve(name);

            IDb2EntityFactory queryEntityFactory = new EfLazyLoadingProxyDb2EntityFactory(_context, new ReflectionDb2EntityFactory());
            var provider = new Db2QueryProvider<TEntity, TRow>(file, model, TableResolver, queryEntityFactory);

            var db2EntityType = model.GetEntityType(typeof(TEntity)).WithSchema(tableName, schema);
            _ = db2EntityType;
            var rootQueryable = new Db2Queryable<TEntity>(provider);

            var rewritten = new RootQueryRewriter<TEntity>(rootQueryable).Visit(query);
            var result = provider.Execute<IEnumerable<TElement>>(rewritten!);

            return (result, session);
        });

        return enumerable;
    }

    private static bool TryGetEnumerableElementType(Type resultType, out Type elementType)
    {
        if (resultType == typeof(string))
        {
            elementType = null!;
            return false;
        }

        if (resultType.IsGenericType)
        {
            var args = resultType.GetGenericArguments();
            if (args.Length == 1 && typeof(IEnumerable<>).MakeGenericType(args[0]).IsAssignableFrom(resultType))
            {
                elementType = args[0];
                return true;
            }
        }

        foreach (var i in resultType.GetInterfaces())
        {
            if (!i.IsGenericType)
                continue;

            if (i.GetGenericTypeDefinition() != typeof(IEnumerable<>))
                continue;

            elementType = i.GetGenericArguments()[0];
            return true;
        }

        elementType = null!;
        return false;
    }

    private static bool TryGetPrimaryKeyMemberName(Db2ModelBinding model, Type entityClrType, out string memberName)
    {
        var entityType = model.GetEntityType(entityClrType);
        memberName = entityType.PrimaryKeyMember.Name;
        return true;
    }

    private static TResult MaterializeEnumerableResult<TEntity, TResult>(IReadOnlyList<TEntity> entities)
        where TEntity : class
    {
        // This handles e.g. Where(x => x.Id == const) without a scalar terminal operator.
        if (entities.Count == 0)
        {
            if (typeof(TResult).IsAssignableFrom(typeof(IEnumerable<TEntity>)))
                return (TResult)(object)Array.Empty<TEntity>();

            // Fallback for unexpected result shapes.
            return default!;
        }

        var array = entities as TEntity[] ?? entities.ToArray();
        if (typeof(TResult).IsAssignableFrom(array.GetType()))
            return (TResult)(object)array;

        return (TResult)(object)array;
    }

    private static bool TryGetKeyLookupRequest<TEntity>(Expression query, string pkMemberName, out KeyLookupRequest request)
    {
        var pipeline = Db2QueryPipeline.Parse(query);

        // Skip remains disqualifying for the key lookup path.
        if (pipeline.Operations.Any(op => op is Db2SkipOperation))
        {
            request = default;
            return false;
        }

        var take = pipeline.Operations.OfType<Db2TakeOperation>().FirstOrDefault();

        // Key lookup can apply Take safely since it controls result ordering.
        // Keep Skip disqualifying for now.
        if (take is not null && take.Count < 0)
        {
            request = default;
            return false;
        }

        var whereOps = pipeline.Operations
            .OfType<Db2WhereOperation>()
            .Where(w => w.Predicate.Parameters is { Count: 1 } && w.Predicate.Parameters[0].Type == typeof(TEntity))
            .ToList();

        if (whereOps.Count != 1)
        {
            request = default;
            return false;
        }

        if (!TryExtractPkIds<TEntity>((Expression<Func<TEntity, bool>>)whereOps[0].Predicate, pkMemberName, out var ids))
        {
            request = default;
            return false;
        }

        var finalPredicate = pipeline.FinalOperator == Db2FinalOperator.All ? pipeline.FinalPredicate : null;
        var includes = pipeline.Operations.OfType<Db2IncludeOperation>().ToArray();

        Db2SelectOperation? select = null;
        for (var i = 0; i < pipeline.Operations.Count; i++)
        {
            if (pipeline.Operations[i] is Db2SelectOperation s)
            {
                if (select is not null)
                {
                    request = default;
                    return false;
                }

                if (s.Selector is not { Parameters.Count: 1 } || s.Selector.Parameters[0].Type != typeof(TEntity))
                {
                    request = default;
                    return false;
                }

                select = s;
            }
        }

        // Include must appear before Select (same as provider behavior).
        if (select is not null)
        {
            var sawSelect = false;
            for (var i = 0; i < pipeline.Operations.Count; i++)
            {
                var op = pipeline.Operations[i];
                if (op == select)
                    sawSelect = true;
                else if (sawSelect && op is Db2IncludeOperation)
                {
                    request = default;
                    return false;
                }
            }
        }

        request = new KeyLookupRequest(ids, take?.Count, pipeline.FinalOperator, finalPredicate, includes, select);
        return true;
    }

    private static IReadOnlyList<TEntity> MaterializeByIdsFromOpenFile<TEntity>(
        IDb2File<RowHandle> file,
        Db2TableSchema schema,
        string tableName,
        IReadOnlyList<int> ids,
        int? takeCount,
        Db2ModelBinding model,
        IDb2EntityFactory entityFactory)
        where TEntity : class
    {
        if (takeCount is 0 || ids.Count == 0)
            return Array.Empty<TEntity>();

        if (takeCount is < 0)
            throw new ArgumentOutOfRangeException(nameof(takeCount), "Take count cannot be negative.");

        var maxCount = takeCount ?? int.MaxValue;

        var handles = new List<RowHandle>(capacity: Math.Min(ids.Count, maxCount));
        for (var i = 0; i < ids.Count; i++)
        {
            if (handles.Count >= maxCount)
                break;

            if (!file.TryGetRowById(ids[i], out var handle))
                continue;

            handles.Add(handle);
        }

        if (handles.Count == 0)
            return Array.Empty<TEntity>();

        handles.Sort(static (a, b) =>
        {
            var section = a.SectionIndex.CompareTo(b.SectionIndex);
            if (section != 0)
                return section;

            return a.RowIndexInSection.CompareTo(b.RowIndexInSection);
        });

        var db2EntityType = model.GetEntityType(typeof(TEntity)).WithSchema(tableName, schema);
        var materializer = new Db2EntityMaterializer<TEntity, RowHandle>(db2EntityType, entityFactory);

        var results = new List<TEntity>(handles.Count);
        for (var i = 0; i < handles.Count; i++)
            results.Add(materializer.Materialize(file, handles[i]));

        return results;
    }

    private static IEnumerable ApplySelect(IEnumerable source, Type sourceElementType, LambdaExpression selector)
    {
        var resultType = selector.ReturnType;
        var typedSelector = selector.Compile();
        return EnumerableDispatch.GetSelect(sourceElementType, resultType)(source, typedSelector);
    }

    private static TResult MaterializeSequenceResult<TResult>(IEnumerable sequence, Type elementType)
    {
        var array = EnumerableDispatch.GetToArray(elementType)(sequence);
        if (typeof(TResult).IsAssignableFrom(array.GetType()))
            return (TResult)array;

        var enumerableType = typeof(IEnumerable<>).MakeGenericType(elementType);
        if (typeof(TResult).IsAssignableFrom(enumerableType))
            return (TResult)array;

        return (TResult)array;
    }

    private static object? ApplyScalarOperator(Db2FinalOperator op, IEnumerable sequence, Type elementType, LambdaExpression? finalPredicate)
    {
        return op switch
        {
            Db2FinalOperator.First => EnumerableDispatch.GetFirst(elementType)(sequence),
            Db2FinalOperator.FirstOrDefault => EnumerableDispatch.GetFirstOrDefault(elementType)(sequence),
            Db2FinalOperator.Single => EnumerableDispatch.GetSingle(elementType)(sequence),
            Db2FinalOperator.SingleOrDefault => EnumerableDispatch.GetSingleOrDefault(elementType)(sequence),
            Db2FinalOperator.Any => EnumerableDispatch.GetAny(elementType)(sequence),
            Db2FinalOperator.Count => EnumerableDispatch.GetCount(elementType)(sequence),
            Db2FinalOperator.All => finalPredicate is null
                ? throw new NotSupportedException("Queryable.All requires a predicate for this provider.")
                : EnumerableDispatch.GetAll(elementType)(sequence, finalPredicate.Compile()),
            _ => throw new NotSupportedException($"Unsupported scalar operator: {op}.")
        };
    }

    private static bool TryExtractPkIds<TEntity>(Expression<Func<TEntity, bool>> predicate, string pkMemberName, out int[] ids)
    {
        // Supports shapes like:
        //  - x => x.Id == 123
        //  - x => 123 == x.Id
        //  - x => EF.Property<int>(x, "Id") == 123
        //  - x => new[] { 1, 2, 3 }.Contains(x.Id)
        //  - x => x.Id == 1 || x.Id == 2

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
                    {
                        // Preserve evaluation order: left ids, then right ids.
                        return TryExtractIdsFromExpression(or.Left, param, pkMemberName, ids)
                               && TryExtractIdsFromExpression(or.Right, param, pkMemberName, ids);
                    }

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

            // Enumerable.Contains(collection, value)
            if (call.Method.DeclaringType == typeof(Enumerable) && call.Arguments.Count == 2)
            {
                collectionExpr = call.Arguments[0];
                valueExpr = call.Arguments[1];
            }
            // instance.Contains(value)
            else if (call.Object is not null && call.Arguments.Count == 1)
            {
                collectionExpr = call.Object;
                valueExpr = call.Arguments[0];
            }
            // Static Contains with two args (e.g. List.Contains as static unlikely but handle).
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

        static bool TryEvaluateIntSequence(Expression expr, ParameterExpression param, out IReadOnlyList<int> values)
        {
            expr = StripConvert(expr);

            // new[] { ... }
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

    private static bool IsKeyAccess(Expression expr, ParameterExpression param, string pkMemberName)
    {
        if (expr is MemberExpression { Member: { Name: var name }, Expression: var instance } && instance == param && name == pkMemberName)
            return true;

        // EF.Property<T>(entity, "Id")
        if (expr is MethodCallExpression { Method: { Name: "Property", DeclaringType: { Name: "EF", Namespace: "Microsoft.EntityFrameworkCore" } }, Arguments: [var entityExpr, var nameExpr] })
        {
            if (entityExpr == param && nameExpr is ConstantExpression { Value: string s } && s == pkMemberName)
                return true;
        }

        return false;
    }

    private readonly record struct KeyLookupRequest(
        int[] Ids,
        int? TakeCount,
        Db2FinalOperator FinalOperator,
        LambdaExpression? FinalPredicate,
        IReadOnlyList<Db2IncludeOperation> Includes,
        Db2SelectOperation? Select);

    private static class EnumerableDispatch
    {
        private static readonly ConcurrentDictionary<(Type Source, Type Result), Func<IEnumerable, Delegate, IEnumerable>> SelectDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<IEnumerable, object>> ToArrayDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<IEnumerable, object?>> FirstDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<IEnumerable, object?>> FirstOrDefaultDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<IEnumerable, object?>> SingleDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<IEnumerable, object?>> SingleOrDefaultDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<IEnumerable, bool>> AnyDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<IEnumerable, int>> CountDelegates = new();
        private static readonly ConcurrentDictionary<Type, Func<IEnumerable, Delegate, bool>> AllDelegates = new();

        public static Func<IEnumerable, Delegate, IEnumerable> GetSelect(Type sourceType, Type resultType)
            => SelectDelegates.GetOrAdd((sourceType, resultType), static key =>
            {
                var m = typeof(Enumerable)
                    .GetMethods(BindingFlags.Public | BindingFlags.Static)
                    .Single(x =>
                        x.Name == nameof(Enumerable.Select)
                        && x.GetParameters().Length == 2
                        && x.GetParameters()[1].ParameterType.IsGenericType
                        && x.GetParameters()[1].ParameterType.GetGenericTypeDefinition() == typeof(Func<,>))
                    .MakeGenericMethod(key.Source, key.Result);

                return (source, selector) => (IEnumerable)m.Invoke(null, [source, selector])!;
            });

        public static Func<IEnumerable, object> GetToArray(Type elementType)
            => ToArrayDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(Enumerable)
                    .GetMethods(BindingFlags.Public | BindingFlags.Static)
                    .Single(x => x.Name == nameof(Enumerable.ToArray) && x.GetParameters().Length == 1)
                    .MakeGenericMethod(elementType);

                return source => m.Invoke(null, [source])!;
            });

        public static Func<IEnumerable, object?> GetFirst(Type elementType)
            => FirstDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(Enumerable)
                    .GetMethods(BindingFlags.Public | BindingFlags.Static)
                    .Single(x => x.Name == nameof(Enumerable.First) && x.GetParameters().Length == 1)
                    .MakeGenericMethod(elementType);

                return source => m.Invoke(null, [source]);
            });

        public static Func<IEnumerable, object?> GetFirstOrDefault(Type elementType)
            => FirstOrDefaultDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(Enumerable)
                    .GetMethods(BindingFlags.Public | BindingFlags.Static)
                    .Single(x => x.Name == nameof(Enumerable.FirstOrDefault) && x.GetParameters().Length == 1)
                    .MakeGenericMethod(elementType);

                return source => m.Invoke(null, [source]);
            });

        public static Func<IEnumerable, object?> GetSingle(Type elementType)
            => SingleDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(Enumerable)
                    .GetMethods(BindingFlags.Public | BindingFlags.Static)
                    .Single(x => x.Name == nameof(Enumerable.Single) && x.GetParameters().Length == 1)
                    .MakeGenericMethod(elementType);

                return source => m.Invoke(null, [source]);
            });

        public static Func<IEnumerable, object?> GetSingleOrDefault(Type elementType)
            => SingleOrDefaultDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(Enumerable)
                    .GetMethods(BindingFlags.Public | BindingFlags.Static)
                    .Single(x => x.Name == nameof(Enumerable.SingleOrDefault) && x.GetParameters().Length == 1)
                    .MakeGenericMethod(elementType);

                return source => m.Invoke(null, [source]);
            });

        public static Func<IEnumerable, bool> GetAny(Type elementType)
            => AnyDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(Enumerable)
                    .GetMethods(BindingFlags.Public | BindingFlags.Static)
                    .Single(x => x.Name == nameof(Enumerable.Any) && x.GetParameters().Length == 1)
                    .MakeGenericMethod(elementType);

                return source => (bool)m.Invoke(null, [source])!;
            });

        public static Func<IEnumerable, int> GetCount(Type elementType)
            => CountDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(Enumerable)
                    .GetMethods(BindingFlags.Public | BindingFlags.Static)
                    .Single(x => x.Name == nameof(Enumerable.Count) && x.GetParameters().Length == 1)
                    .MakeGenericMethod(elementType);

                return source => (int)m.Invoke(null, [source])!;
            });

        public static Func<IEnumerable, Delegate, bool> GetAll(Type elementType)
            => AllDelegates.GetOrAdd(elementType, static elementType =>
            {
                var m = typeof(Enumerable)
                    .GetMethods(BindingFlags.Public | BindingFlags.Static)
                    .Single(x => x.Name == nameof(Enumerable.All) && x.GetParameters().Length == 2)
                    .MakeGenericMethod(elementType);

                return (source, predicate) => (bool)m.Invoke(null, [source, predicate])!;
            });
    }

    private static Type GetRootEntityClrType(Expression expression)
    {
        var current = expression;
        while (current is MethodCallExpression m)
            current = m.Arguments[0];

        return GetSequenceElementType(current.Type) ?? throw new NotSupportedException("Unable to determine root entity type for this query.");
    }

    private static Type? GetSequenceElementType(Type sequenceType)
        => sequenceType.IsGenericType && sequenceType.GetGenericTypeDefinition() == typeof(IEnumerable<>)
            ? sequenceType.GetGenericArguments()[0]
            : sequenceType.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                ?.GetGenericArguments()[0];

    private sealed class RootQueryRewriter<TEntity>(IQueryable<TEntity> root) : ExpressionVisitor
    {
        private readonly IQueryable<TEntity> _root = root;

        protected override Expression VisitExtension(Expression node)
        {
            if (node is EntityQueryRootExpression eqr && eqr.EntityType.ClrType == typeof(TEntity))
                return Expression.Constant(_root, node.Type);

            return base.VisitExtension(node);
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Value is IQueryable q
                && q.ElementType == typeof(TEntity)
                && q.Expression is EntityQueryRootExpression eqr
                && eqr.EntityType.ClrType == typeof(TEntity))
            {
                return Expression.Constant(_root, typeof(IQueryable<TEntity>));
            }

            return base.VisitConstant(node);
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
}
