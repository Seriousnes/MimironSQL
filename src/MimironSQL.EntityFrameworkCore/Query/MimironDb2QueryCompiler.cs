using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
#pragma warning disable EF1001 // Internal EF Core API usage is intentional for provider implementation.
using Microsoft.EntityFrameworkCore.Query.Internal;
#pragma warning restore EF1001

using MimironSQL.Db2.Query;
using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;

namespace MimironSQL.EntityFrameworkCore.Query;

#pragma warning disable EF1001 // Internal EF Core API usage is intentional for provider implementation.
internal sealed class MimironDb2QueryCompiler(
    ICurrentDbContext currentDbContext,
    IMimironDb2Store store,
    IMimironDb2Db2ModelProvider db2ModelProvider) : IQueryCompiler
{
    private static readonly ConcurrentDictionary<(Type EntityType, Type RowType, Type ResultType), Func<MimironDb2QueryCompiler, Expression, object?>> ExecuteDelegates = new();
    private static readonly ConcurrentDictionary<(Type EntityType, Type RowType), Func<string, Db2TableSchema, Db2EntityType, IQueryProvider, IDb2File, IQueryable>> TableFactories = new();

    private readonly DbContext _context = currentDbContext?.Context ?? throw new ArgumentNullException(nameof(currentDbContext));
    private readonly IMimironDb2Store _store = store ?? throw new ArgumentNullException(nameof(store));
    private readonly IMimironDb2Db2ModelProvider _db2ModelProvider = db2ModelProvider ?? throw new ArgumentNullException(nameof(db2ModelProvider));

    public TResult Execute<TResult>(Expression query)
    {
        ArgumentNullException.ThrowIfNull(query);

        var rootEntityType = GetRootEntityClrType(query);

        var efEntityType = _context.Model.FindEntityType(rootEntityType)
            ?? throw new NotSupportedException($"Entity type '{rootEntityType.FullName}' is not part of the EF model.");

        var tableName = efEntityType.GetTableName() ?? rootEntityType.Name;
        var (file, _) = _store.OpenTableWithSchema(tableName);

        var rowType = file.RowType;
        if (rowType is null)
            throw new InvalidOperationException($"DB2 file for table '{tableName}' did not specify a row type.");

        var model = _db2ModelProvider.GetDb2Model();

        var result = ExecuteDelegates.GetOrAdd((rootEntityType, rowType, typeof(TResult)), static key =>
        {
            var method = typeof(MimironDb2QueryCompiler)
                .GetMethod(nameof(ExecuteTyped), BindingFlags.NonPublic | BindingFlags.Instance)!
                .MakeGenericMethod(key.EntityType, key.RowType, key.ResultType);

            return method.CreateDelegate<Func<MimironDb2QueryCompiler, Expression, object?>>();
        })(this, query);

        return (TResult)result!;
    }

    private object? ExecuteTyped<TEntity, TRow, TResult>(Expression query)
        where TRow : struct, IRowHandle
    {
        var efEntityType = _context.Model.FindEntityType(typeof(TEntity))
            ?? throw new NotSupportedException($"Entity type '{typeof(TEntity).FullName}' is not part of the EF model.");

        var tableName = efEntityType.GetTableName() ?? typeof(TEntity).Name;
        var (file, schema) = _store.OpenTableWithSchema<TRow>(tableName);

        (IDb2File<TRow> File, Db2TableSchema Schema) TableResolver(string name)
            => _store.OpenTableWithSchema<TRow>(name);

        var model = _db2ModelProvider.GetDb2Model();
        var provider = new Db2QueryProvider<TEntity, TRow>(file, model, TableResolver);

        var db2EntityType = model.GetEntityType(typeof(TEntity)).WithSchema(tableName, schema);
        var rootQueryable = CreateTableQueryable<TEntity, TRow>(tableName, schema, db2EntityType, provider, file);

        var rewritten = new RootQueryRewriter<TEntity>(rootQueryable).Visit(query);
        return provider.Execute<TResult>(rewritten!);
    }

    private static IQueryable<TEntity> CreateTableQueryable<TEntity, TRow>(
        string tableName,
        Db2TableSchema schema,
        Db2EntityType entityType,
        IQueryProvider provider,
        IDb2File<TRow> file)
        where TRow : struct
    {
        var factory = TableFactories.GetOrAdd((typeof(TEntity), typeof(TRow)), static key =>
        {
            var openGeneric = typeof(Db2QueryProvider<,>).Assembly.GetType("MimironSQL.Db2.Query.Db2Table`2")
                ?? throw new InvalidOperationException("Unable to locate MimironSQL.Db2.Query.Db2Table`2.");

            var closed = openGeneric.MakeGenericType(key.EntityType, key.RowType);
            var ctor = closed.GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic)
                .SingleOrDefault(c => c.GetParameters().Length == 5)
                ?? throw new InvalidOperationException($"Unable to locate internal Db2Table<,> constructor for {closed.FullName}.");

            return (string tableName, Db2TableSchema schema, Db2EntityType entityType, IQueryProvider provider, IDb2File file)
                => (IQueryable)ctor.Invoke([tableName, schema, entityType, provider, file]);
        });

        return (IQueryable<TEntity>)factory(tableName, schema, entityType, provider, file);
    }

    public Func<QueryContext, TResult> CreateCompiledQuery<TResult>(Expression query)
        => _ => Execute<TResult>(query);

    public Func<QueryContext, TResult> CreateCompiledAsyncQuery<TResult>(Expression query)
        => _ => throw new NotSupportedException("Async query execution is not supported.");

    public TResult ExecuteAsync<TResult>(Expression query, CancellationToken cancellationToken)
        => throw new NotSupportedException("Async query execution is not supported.");

    public Expression<Func<QueryContext, TResult>> PrecompileQuery<TResult>(Expression query, bool async)
        => throw new NotSupportedException("Query precompilation is not supported.");

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
            if (node.Type.IsGenericType
                && node.Type.GetGenericTypeDefinition().FullName == "Microsoft.EntityFrameworkCore.Query.Internal.EntityQueryable`1"
                && node.Type.GetGenericArguments()[0] == typeof(TEntity))
            {
                return Expression.Constant(_root, typeof(IQueryable<TEntity>));
            }

            return base.VisitConstant(node);
        }
    }
}
#pragma warning restore EF1001
