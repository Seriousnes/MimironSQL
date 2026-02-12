using System.Collections.Concurrent;
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
    IMimironDb2Db2ModelProvider db2ModelProvider) : IMimironDb2QueryExecutor
{
    private static readonly ConcurrentDictionary<(Type EntityType, Type RowType, Type ResultType), Func<MimironDb2QueryExecutor, Expression, object?>> ExecuteDelegates = new();

    private readonly DbContext _context = currentDbContext?.Context ?? throw new ArgumentNullException(nameof(currentDbContext));
    private readonly IMimironDb2Store _store = store ?? throw new ArgumentNullException(nameof(store));
    private readonly IMimironDb2Db2ModelProvider _db2ModelProvider = db2ModelProvider ?? throw new ArgumentNullException(nameof(db2ModelProvider));

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

        var model = _db2ModelProvider.GetDb2Model();

        var pipeline = Db2QueryPipeline.Parse(query);

        if (typeof(TRow) == typeof(RowHandle)
            && TryGetPrimaryKeyMemberName(model, typeof(TEntity), out var pkMemberName)
            && TryGetKeyLookupRequest<TEntity>(query, pkMemberName, out var request))
        {
            IDb2EntityFactory keyLookupEntityFactory = new EfLazyLoadingProxyDb2EntityFactory(_context, new ReflectionDb2EntityFactory());

            var efEntityType2 = _context.Model.FindEntityType(typeof(TEntity))
                ?? throw new NotSupportedException($"Entity type '{typeof(TEntity).FullName}' is not part of the EF model.");

            var tableName2 = efEntityType2.GetTableName() ?? typeof(TEntity).Name;

            var found = _store.TryMaterializeById<TEntity>(tableName2, request.Id, model, keyLookupEntityFactory, out var entity);

            return request.FinalOperator switch
            {
                Db2FinalOperator.None => (object)MaterializeEnumerableResult<TEntity, TResult>(found, entity),
                Db2FinalOperator.FirstOrDefault => entity,
                Db2FinalOperator.SingleOrDefault => entity,
                Db2FinalOperator.First => found ? entity : throw new InvalidOperationException("Sequence contains no elements"),
                Db2FinalOperator.Single => found ? entity : throw new InvalidOperationException("Sequence contains no elements"),
                Db2FinalOperator.Any => found,
                Db2FinalOperator.Count => found ? 1 : 0,
                Db2FinalOperator.All => throw new NotSupportedException("Key lookup fast-path does not support All."),
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

    private object ExecuteDeferredEnumerable<TEntity, TRow>(Expression query, Db2Model model, string tableName, Type elementType)
        where TEntity : class
        where TRow : struct, IRowHandle
    {
        var method = typeof(MimironDb2QueryExecutor)
            .GetMethod(nameof(ExecuteDeferredEnumerableTyped), BindingFlags.NonPublic | BindingFlags.Instance)!
            .MakeGenericMethod(typeof(TEntity), typeof(TRow), elementType);

        return method.Invoke(this, [query, model, tableName])!;
    }

    private object ExecuteDeferredEnumerableTyped<TEntity, TRow, TElement>(Expression query, Db2Model model, string tableName)
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

    private static bool TryGetPrimaryKeyMemberName(Db2Model model, Type entityClrType, out string memberName)
    {
        var entityType = model.GetEntityType(entityClrType);
        memberName = entityType.PrimaryKeyMember.Name;
        return true;
    }

    private static TResult MaterializeEnumerableResult<TEntity, TResult>(bool found, TEntity? entity)
        where TEntity : class
    {
        // This handles e.g. Where(x => x.Id == const) without a scalar terminal operator.
        if (!found || entity is null)
        {
            if (typeof(TResult).IsAssignableFrom(typeof(IEnumerable<TEntity>)))
                return (TResult)(object)Enumerable.Empty<TEntity>();

            // Fallback for unexpected result shapes.
            return default!;
        }

        var one = new[] { entity };
        if (typeof(TResult).IsAssignableFrom(one.GetType()))
            return (TResult)(object)one;

        return (TResult)(object)one;
    }

    private static bool TryGetKeyLookupRequest<TEntity>(Expression query, string pkMemberName, out KeyLookupRequest request)
    {
        var pipeline = Db2QueryPipeline.Parse(query);

        // Key lookup path only supports root-entity sequences without Include/Select.
        if (pipeline.Operations.Any(op => op is Db2IncludeOperation or Db2SelectOperation or Db2SkipOperation))
        {
            request = default;
            return false;
        }

        var take = pipeline.Operations.OfType<Db2TakeOperation>().FirstOrDefault();
        if (take is not null && take.Count != 1)
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

        if (!TryExtractIdEquality<TEntity>((Expression<Func<TEntity, bool>>)whereOps[0].Predicate, pkMemberName, out var id))
        {
            request = default;
            return false;
        }

        request = new KeyLookupRequest(id, pipeline.FinalOperator);
        return true;
    }

    private static bool TryExtractIdEquality<TEntity>(Expression<Func<TEntity, bool>> predicate, string pkMemberName, out int id)
    {
        // Support shapes like:
        //  - x => x.Id == 123
        //  - x => 123 == x.Id
        //  - x => EF.Property<int>(x, "Id") == 123

        static Expression StripConvert(Expression e)
            => e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u ? u.Operand : e;

        id = 0;

        if (predicate.Body is not BinaryExpression { NodeType: ExpressionType.Equal } eq)
            return false;

        var left = StripConvert(eq.Left);
        var right = StripConvert(eq.Right);

        if (TryGetConstantInt(right, out var rightConst) && IsKeyAccess(left, predicate.Parameters[0], pkMemberName))
        {
            id = rightConst;
            return true;
        }

        if (TryGetConstantInt(left, out var leftConst) && IsKeyAccess(right, predicate.Parameters[0], pkMemberName))
        {
            id = leftConst;
            return true;
        }

        return false;
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

    private static bool TryGetConstantInt(Expression expr, out int value)
    {
        if (expr is ConstantExpression { Value: int i })
        {
            value = i;
            return true;
        }

        if (expr is ConstantExpression { Value: null })
        {
            value = 0;
            return false;
        }

        value = 0;
        return false;
    }

    private readonly record struct KeyLookupRequest(int Id, Db2FinalOperator FinalOperator);

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
}
