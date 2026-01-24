using System.Linq.Expressions;

namespace MimironSQL.Db2.Query;

public static class Db2QueryableExtensions
{
    public static IQueryable<TEntity> Include<TEntity, TProperty>(
        this IQueryable<TEntity> source,
        Expression<Func<TEntity, TProperty>> navigationPropertyPath)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(navigationPropertyPath);

        var method = ((MethodCallExpression)((Expression<Func<IQueryable<TEntity>>>)(
            () => Include(source, navigationPropertyPath))).Body).Method;

        return source.Provider.CreateQuery<TEntity>(
            Expression.Call(
                instance: null,
                method: method,
                arguments: [source.Expression, Expression.Quote(navigationPropertyPath)]));
    }
}
