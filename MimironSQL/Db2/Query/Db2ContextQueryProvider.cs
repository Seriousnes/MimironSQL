using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Db2.Model;

using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace MimironSQL.Db2.Query;

internal sealed class Db2ContextQueryProvider(Db2Context context) : IQueryProvider
{
    private static readonly ConcurrentDictionary<Type, Func<IQueryProvider, Expression, IQueryable>> QueryableFactories = new();
    private static readonly ConcurrentDictionary<Type, Func<Wdc5File, Db2TableSchema, Db2Model, Func<string, (Wdc5File File, Db2TableSchema Schema)>, IQueryProvider>> PerTableProviderFactories = new();

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

        var provider = PerTableProviderFactories.GetOrAdd(entityType, static entityType =>
        {
            var factoryMethod = typeof(Db2ContextQueryProvider)
                .GetMethod(nameof(CreatePerTableProvider), System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)!;

            var generic = factoryMethod.MakeGenericMethod(entityType);
            return (Func<Wdc5File, Db2TableSchema, Db2Model, Func<string, (Wdc5File File, Db2TableSchema Schema)>, IQueryProvider>)
                generic.CreateDelegate(typeof(Func<Wdc5File, Db2TableSchema, Db2Model, Func<string, (Wdc5File File, Db2TableSchema Schema)>, IQueryProvider>));
        })(file, schema, _context.Model, _context.GetOrOpenTableRaw);

        return provider.Execute<TResult>(expression);
    }

    private static IQueryable CreateQueryable<TElement>(IQueryProvider provider, Expression expression)
        => new Db2Queryable<TElement>(provider, expression);

    private static IQueryProvider CreatePerTableProvider<TEntity>(
        Wdc5File file,
        Db2TableSchema schema,
        Db2Model model,
        Func<string, (Wdc5File File, Db2TableSchema Schema)> tableResolver)
        => new Db2QueryProvider<TEntity>(file, schema, model, tableResolver);

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
