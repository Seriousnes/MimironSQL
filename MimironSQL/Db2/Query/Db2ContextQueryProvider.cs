using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Model;
using MimironSQL.Formats;

using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace MimironSQL.Db2.Query;

internal sealed class Db2ContextQueryProvider(Db2Context context) : IQueryProvider
{
    private static readonly ConcurrentDictionary<Type, Func<IQueryProvider, Expression, IQueryable>> QueryableFactories = new();
    private static readonly ConcurrentDictionary<(Type EntityType, Type RowType), Func<Db2Context, IDb2File, Db2TableSchema, Db2Model, IQueryProvider>> PerTableProviderFactories = new();

    private readonly Db2Context _context = context;

    public IQueryable CreateQuery(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        var elementType = expression.Type.GetGenericArguments().FirstOrDefault() ?? typeof(object);

        return QueryableFactories.GetOrAdd(elementType, static elementType =>
        {
            var factoryMethod = typeof(Db2ContextQueryProvider)
                .GetMethod(nameof(CreateQueryable), System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)!;

            var generic = factoryMethod.MakeGenericMethod(elementType);
            return (Func<IQueryProvider, Expression, IQueryable>)generic.CreateDelegate(typeof(Func<IQueryProvider, Expression, IQueryable>));
        })(this, expression);
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        return new Db2Queryable<TElement>(this, expression);
    }

    public object? Execute(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        // Non-generic IQueryProvider.Execute returns object, which necessarily boxes scalar value types.
        // This provider expects callers to use Execute<TResult>.
        throw new NotSupportedException("Use Execute<TResult>(...) instead of the non-generic Execute(...) for this provider.");
    }

    public TResult Execute<TResult>(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        var (entityType, tableName) = GetRootTable(expression);
        var (file, schema) = _context.GetOrOpenTableRaw(tableName);

        var provider = PerTableProviderFactories.GetOrAdd((entityType, file.RowType), static key =>
        {
            var factoryMethod = typeof(Db2ContextQueryProvider)
                .GetMethod(nameof(CreatePerTableProvider), System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)!;

            var generic = factoryMethod.MakeGenericMethod(key.EntityType, key.RowType);
            return (Func<Db2Context, IDb2File, Db2TableSchema, Db2Model, IQueryProvider>)
                generic.CreateDelegate(typeof(Func<Db2Context, IDb2File, Db2TableSchema, Db2Model, IQueryProvider>));
        })(_context, file, schema, _context.Model);

        return provider.Execute<TResult>(expression);
    }

    private static IQueryable CreateQueryable<TElement>(IQueryProvider provider, Expression expression)
        => new Db2Queryable<TElement>(provider, expression);

    private static IQueryProvider CreatePerTableProvider<TEntity, TRow>(
        Db2Context context,
        IDb2File file,
        Db2TableSchema schema,
        Db2Model model)
        where TRow : struct, IDb2Row
    {
        var typedFile = (IDb2File<TRow>)file;
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> resolver = tableName => context.GetOrOpenTableRawTyped<TRow>(tableName);
        return new Db2QueryProvider<TEntity, TRow>(typedFile, schema, model, resolver);
    }

    private static (Type EntityType, string TableName) GetRootTable(Expression expression)
    {
        var current = expression;
        while (current is MethodCallExpression m)
            current = m.Arguments[0];

        if (current is ConstantExpression { Value: IDb2Table table })
            return (table.EntityType, table.TableName);

        throw new NotSupportedException("Unable to locate the root Db2Table for this query.");
    }
}
