using System.Collections;
using System.Linq.Expressions;

namespace MimironSQL.EntityFrameworkCore.Query;

internal sealed class MimironDb2Queryable<T> : IQueryable<T>, IAsyncEnumerable<T>
{
    public MimironDb2Queryable(IQueryProvider provider, Expression expression)
    {
        Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        Expression = expression ?? throw new ArgumentNullException(nameof(expression));
    }

    public Type ElementType => typeof(T);
    public Expression Expression { get; }
    public IQueryProvider Provider { get; }

    public IEnumerator<T> GetEnumerator()
        => Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var enumerable = Provider.Execute<IEnumerable<T>>(Expression);
        return new AsyncEnumeratorWrapper<T>(enumerable.GetEnumerator());
    }

    private sealed class AsyncEnumeratorWrapper<TItem> : IAsyncEnumerator<TItem>
    {
        private readonly IEnumerator<TItem> _inner;

        public AsyncEnumeratorWrapper(IEnumerator<TItem> inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        }

        public TItem Current => _inner.Current;

        public ValueTask<bool> MoveNextAsync()
            => new(_inner.MoveNext());

        public ValueTask DisposeAsync()
        {
            _inner.Dispose();
            return default;
        }
    }
}
