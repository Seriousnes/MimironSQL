using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Db2.Model;
using MimironSQL.Formats;

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

        var (entityType, tableName) = GetRootTable(expression);

        var (file, schema) = _context.GetOrOpenTableRaw(tableName);
        var wdc5File = (Wdc5File)file;

        var provider = CreatePerTableProvider(entityType, wdc5File, schema, _context.Model, _context.GetOrOpenTableRaw);
        return provider.Execute(expression);
    }

    public TResult Execute<TResult>(Expression expression)
        => (TResult)Execute(expression)!;

    private static IQueryProvider CreatePerTableProvider(
        Type entityType,
        Wdc5File file,
        Db2TableSchema schema,
        Db2Model model,
        Func<string, (IDb2File File, Db2TableSchema Schema)> tableResolver)
    {
        var providerType = typeof(Db2QueryProvider<>).MakeGenericType(entityType);
        return (IQueryProvider)Activator.CreateInstance(providerType, file, schema, model, tableResolver)!;
    }

    private static (Type EntityType, string TableName) GetRootTable(Expression expression)
    {
        var current = expression;
        while (current is MethodCallExpression m)
            current = m.Arguments[0];

        if (current is ConstantExpression { Value: { } value })
        {
            var valueType = value.GetType();
            if (valueType is { IsGenericType: true } && valueType.GetGenericTypeDefinition() == typeof(Db2Table<>))
            {
                var entityType = valueType.GetGenericArguments()[0];

                var tableNameProperty = valueType.GetProperty("TableName", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public);
                var tableName = (string?)tableNameProperty?.GetValue(value);
                if (tableName is null)
                    throw new NotSupportedException("Unable to locate the root Db2Table for this query.");

                return (entityType, tableName);
            }
        }

        throw new NotSupportedException("Unable to locate the root Db2Table for this query.");
    }
}
