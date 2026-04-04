using System.Collections;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

internal sealed class Db2QueryingEnumerable<T>(IEnumerable<T> inner)
    : IEnumerable<T>, IAsyncEnumerable<T>
{
    public IEnumerator<T> GetEnumerator() => inner.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => inner.GetEnumerator();

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        => new AsyncEnumeratorAdapter(inner.GetEnumerator(), cancellationToken);

    private sealed class AsyncEnumeratorAdapter(IEnumerator<T> enumerator, CancellationToken cancellationToken)
        : IAsyncEnumerator<T>
    {
        public T Current => enumerator.Current;

        public ValueTask<bool> MoveNextAsync()
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new(enumerator.MoveNext());
        }

        public ValueTask DisposeAsync()
        {
            enumerator.Dispose();
            return default;
        }
    }
}
