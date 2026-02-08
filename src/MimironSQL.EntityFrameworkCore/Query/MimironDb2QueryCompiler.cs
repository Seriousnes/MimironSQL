using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;

using MimironSQL.EntityFrameworkCore.Db2.Query;
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

        var tableName = efEntityType.GetTableName() ?? rootEntityType.Name;
        var (file, _) = _store.OpenTableWithSchema(tableName);

        var rowType = file.RowType ?? throw new InvalidOperationException($"DB2 file for table '{tableName}' did not specify a row type.");
        var model = _db2ModelProvider.GetDb2Model();

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

        var efEntityType = _context.Model.FindEntityType(typeof(TEntity))
            ?? throw new NotSupportedException($"Entity type '{typeof(TEntity).FullName}' is not part of the EF model.");

        var tableName = efEntityType.GetTableName() ?? typeof(TEntity).Name;
        var (file, schema) = _store.OpenTableWithSchema<TRow>(tableName);

        (IDb2File<TRow> File, Db2TableSchema Schema) TableResolver(string name)
            => _store.OpenTableWithSchema<TRow>(name);

        var model = _db2ModelProvider.GetDb2Model();
        IDb2EntityFactory entityFactory = new EfLazyLoadingProxyDb2EntityFactory(_context, new ReflectionDb2EntityFactory());
        var provider = new Db2QueryProvider<TEntity, TRow>(file, model, TableResolver, entityFactory);

        var db2EntityType = model.GetEntityType(typeof(TEntity)).WithSchema(tableName, schema);
        _ = db2EntityType;
        var rootQueryable = new Db2Queryable<TEntity>(provider);

        var rewritten = new RootQueryRewriter<TEntity>(rootQueryable).Visit(query);
        return provider.Execute<TResult>(rewritten!);
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
}
