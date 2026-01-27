using System.Linq.Expressions;
using System.Reflection;

namespace MimironSQL.Db2.Query;

public static class Db2QueryableExtensions
{
    private static readonly MethodInfo IncludeMethodInfo = 
        typeof(Db2QueryableExtensions).GetMethod(nameof(Include), BindingFlags.Public | BindingFlags.Static)!;

    public static IQueryable<TEntity> Include<TEntity, TProperty>(
        this IQueryable<TEntity> source,
        Expression<Func<TEntity, TProperty>> navigationPropertyPath)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(navigationPropertyPath);

        var method = IncludeMethodInfo.MakeGenericMethod(typeof(TEntity), typeof(TProperty));

        return source.Provider.CreateQuery<TEntity>(
            Expression.Call(
                instance: null,
                method: method,
                arguments: [source.Expression, Expression.Quote(navigationPropertyPath)]));
    }
}
