using System.Collections;

namespace MimironSQL.EntityFrameworkCore.Query;

internal sealed class DeferredScopeEnumerable<T>(Func<(IEnumerable<T> Enumerable, IDisposable Scope)> factory) : IEnumerable<T>
{
    private readonly Func<(IEnumerable<T> Enumerable, IDisposable Scope)> _factory = factory ?? throw new ArgumentNullException(nameof(factory));

    public IEnumerator<T> GetEnumerator()
    {
        var (enumerable, scope) = _factory();
        try
        {
            return new Enumerator(enumerable.GetEnumerator(), scope);
        }
        catch
        {
            scope.Dispose();
            throw;
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private sealed class Enumerator : IEnumerator<T>
    {
        private readonly IEnumerator<T> _inner;
        private IDisposable? _scope;

        public Enumerator(IEnumerator<T> inner, IDisposable scope)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _scope = scope ?? throw new ArgumentNullException(nameof(scope));
        }

        public T Current => _inner.Current;

        object IEnumerator.Current => Current!;

        public bool MoveNext()
        {
            try
            {
                if (_inner.MoveNext())
                    return true;

                Dispose();
                return false;
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        public void Reset() => _inner.Reset();

        public void Dispose()
        {
            var scope = Interlocked.Exchange(ref _scope, null);
            try
            {
                _inner.Dispose();
            }
            finally
            {
                scope?.Dispose();
            }
        }
    }
}
