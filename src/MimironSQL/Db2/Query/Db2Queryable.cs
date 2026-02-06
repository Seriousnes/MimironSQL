using System.Collections;
using System.Linq.Expressions;

namespace MimironSQL.Db2.Query;

internal sealed class Db2Queryable<T>(IQueryProvider provider, Expression expression) : IQueryable<T>
{
    public Type ElementType => typeof(T);
    public Expression Expression { get; } = expression;
    public IQueryProvider Provider { get; } = provider;

    public IEnumerator<T> GetEnumerator()
        => Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
