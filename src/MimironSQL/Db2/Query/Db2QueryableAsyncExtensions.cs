using System.Linq.Expressions;
using System.Runtime.CompilerServices;

namespace MimironSQL.Db2.Query;

public static class Db2QueryableAsyncExtensions
{
    public static Task<List<TSource>> ToListAsync<TSource>(
        this IQueryable<TSource> source,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(source.ToList());
    }

    public static Task<TSource[]> ToArrayAsync<TSource>(
        this IQueryable<TSource> source,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(source.ToArray());
    }

    public static Task<TSource> FirstAsync<TSource>(
        this IQueryable<TSource> source,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(source.First());
    }

    public static Task<TSource> FirstAsync<TSource>(
        this IQueryable<TSource> source,
        Expression<Func<TSource, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(predicate);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(source.Where(predicate).First());
    }

    public static Task<TSource?> FirstOrDefaultAsync<TSource>(
        this IQueryable<TSource> source,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(source.FirstOrDefault());
    }

    public static Task<TSource?> FirstOrDefaultAsync<TSource>(
        this IQueryable<TSource> source,
        Expression<Func<TSource, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(predicate);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(source.Where(predicate).FirstOrDefault());
    }

    public static Task<TSource> SingleAsync<TSource>(
        this IQueryable<TSource> source,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(source.Single());
    }

    public static Task<TSource> SingleAsync<TSource>(
        this IQueryable<TSource> source,
        Expression<Func<TSource, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(predicate);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(source.Where(predicate).Single());
    }

    public static Task<TSource?> SingleOrDefaultAsync<TSource>(
        this IQueryable<TSource> source,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(source.SingleOrDefault());
    }

    public static Task<TSource?> SingleOrDefaultAsync<TSource>(
        this IQueryable<TSource> source,
        Expression<Func<TSource, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(predicate);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(source.Where(predicate).SingleOrDefault());
    }

    public static Task<bool> AnyAsync<TSource>(
        this IQueryable<TSource> source,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(source.Any());
    }

    public static Task<bool> AnyAsync<TSource>(
        this IQueryable<TSource> source,
        Expression<Func<TSource, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(predicate);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(source.Where(predicate).Any());
    }

    public static Task<bool> AllAsync<TSource>(
        this IQueryable<TSource> source,
        Expression<Func<TSource, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(predicate);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(source.All(predicate));
    }

    public static Task<int> CountAsync<TSource>(
        this IQueryable<TSource> source,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(source.Count());
    }

    public static Task<int> CountAsync<TSource>(
        this IQueryable<TSource> source,
        Expression<Func<TSource, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(predicate);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(source.Where(predicate).Count());
    }

    public static async IAsyncEnumerable<TSource> AsAsyncEnumerable<TSource>(
        this IQueryable<TSource> source,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);

        foreach (var item in source)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
        }
    }
}
