using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Db2.Model;

using System.Linq.Expressions;

namespace MimironSQL.Db2.Query;

internal sealed class Db2ContextQueryProvider(Db2Context context) : IQueryProvider
{
    private readonly Db2Context _context = context;

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

        var table = GetRootTable(expression);
        var entityType = table.EntityType;

        var provider = CreatePerTableProvider(entityType, table.File, table.Schema, _context.Model, _context.GetOrOpenTableRaw);
        return provider.Execute(expression);
    }

    public TResult Execute<TResult>(Expression expression)
        => (TResult)Execute(expression)!;

    private static IQueryProvider CreatePerTableProvider(
        Type entityType,
        Wdc5File file,
        Db2TableSchema schema,
        Db2Model model,
        Func<string, (Wdc5File File, Db2TableSchema Schema)> tableResolver)
    {
        var providerType = typeof(Db2QueryProvider<>).MakeGenericType(entityType);
        return (IQueryProvider)Activator.CreateInstance(providerType, file, schema, model, tableResolver)!;
    }

    private static IDb2Table GetRootTable(Expression expression)
    {
        var current = expression;
        while (current is MethodCallExpression m)
            current = m.Arguments[0];

        if (current is ConstantExpression { Value: IDb2Table table })
            return table;

        throw new NotSupportedException("Unable to locate the root Db2Table for this query.");
    }
}
