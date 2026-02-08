using System.Collections;
using System.Linq.Expressions;

namespace MimironSQL.Db2.Query;

internal sealed class Db2Queryable<T>(IQueryProvider provider, Expression expression) : IQueryable<T>, IAsyncEnumerable<T>
{
    public Type ElementType => typeof(T);
    public Expression Expression { get; } = expression;
    public IQueryProvider Provider { get; } = provider;

    public IEnumerator<T> GetEnumerator()
        => Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        => new AsyncEnumerator(Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator(), cancellationToken);

    private sealed class AsyncEnumerator(IEnumerator<T> inner, CancellationToken cancellationToken) : IAsyncEnumerator<T>
    {
        public T Current => inner.Current;

        public ValueTask DisposeAsync()
        {
            inner.Dispose();
            return default;
        }

        public ValueTask<bool> MoveNextAsync()
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(inner.MoveNext());
        }
    }
}
