using System.Linq.Expressions;
using System.Reflection;

namespace MimironSQL.Db2.Query;

public interface IIncludableQueryable<out TEntity, out TProperty> : IQueryable<TEntity>
{
}

public static class Db2QueryableExtensions
{
    private static readonly MethodInfo IncludeMethodInfo =
        typeof(Db2QueryableExtensions).GetMethod(nameof(Include), BindingFlags.Public | BindingFlags.Static)
        ?? throw new InvalidOperationException("Failed to locate Include method on Db2QueryableExtensions.");

    private static readonly MethodInfo ThenIncludeReferenceMethodInfo =
        typeof(Db2QueryableExtensions).GetMethod(nameof(ThenIncludeReference), BindingFlags.Public | BindingFlags.Static)
        ?? throw new InvalidOperationException("Failed to locate ThenIncludeReference method on Db2QueryableExtensions.");

    private static readonly MethodInfo ThenIncludeCollectionMethodInfo =
        typeof(Db2QueryableExtensions).GetMethod(nameof(ThenIncludeCollection), BindingFlags.Public | BindingFlags.Static)
        ?? throw new InvalidOperationException("Failed to locate ThenIncludeCollection method on Db2QueryableExtensions.");

    public static IIncludableQueryable<TEntity, TProperty> Include<TEntity, TProperty>(
        this IQueryable<TEntity> source,
        Expression<Func<TEntity, TProperty>> navigationPropertyPath)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(navigationPropertyPath);

        var method = IncludeMethodInfo.MakeGenericMethod(typeof(TEntity), typeof(TProperty));

        return new IncludableQueryable<TEntity, TProperty>(
            source.Provider.CreateQuery<TEntity>(
                Expression.Call(
                    instance: null,
                    method: method,
                    arguments: [source.Expression, Expression.Quote(navigationPropertyPath)])));
    }

    public static IIncludableQueryable<TEntity, TProperty> ThenIncludeReference<TEntity, TPreviousProperty, TProperty>(
        this IIncludableQueryable<TEntity, TPreviousProperty> source,
        Expression<Func<TPreviousProperty, TProperty>> navigationPropertyPath)
        where TPreviousProperty : class
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(navigationPropertyPath);

        var method = ThenIncludeReferenceMethodInfo.MakeGenericMethod(typeof(TEntity), typeof(TPreviousProperty), typeof(TProperty));

        return new IncludableQueryable<TEntity, TProperty>(
            source.Provider.CreateQuery<TEntity>(
                Expression.Call(
                    instance: null,
                    method: method,
                    arguments: [source.Expression, Expression.Quote(navigationPropertyPath)])));
    }

    public static IIncludableQueryable<TEntity, TProperty> ThenIncludeCollection<TEntity, TPreviousProperty, TProperty>(
        this IIncludableQueryable<TEntity, IEnumerable<TPreviousProperty>> source,
        Expression<Func<TPreviousProperty, TProperty>> navigationPropertyPath)
        where TPreviousProperty : class
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(navigationPropertyPath);

        var method = ThenIncludeCollectionMethodInfo.MakeGenericMethod(typeof(TEntity), typeof(TPreviousProperty), typeof(TProperty));

        return new IncludableQueryable<TEntity, TProperty>(
            source.Provider.CreateQuery<TEntity>(
                Expression.Call(
                    instance: null,
                    method: method,
                    arguments: [source.Expression, Expression.Quote(navigationPropertyPath)])));
    }

    private sealed class IncludableQueryable<TEntity, TProperty>(IQueryable<TEntity> queryable) : IIncludableQueryable<TEntity, TProperty>
    {
        public Type ElementType => queryable.ElementType;
        public Expression Expression => queryable.Expression;
        public IQueryProvider Provider => queryable.Provider;

        public IEnumerator<TEntity> GetEnumerator() => queryable.GetEnumerator();
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
