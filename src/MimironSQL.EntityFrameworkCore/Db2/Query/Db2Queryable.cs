using System.Collections;
using System.Linq.Expressions;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

internal sealed class Db2Queryable<T> : IQueryable<T>
{
    public Type ElementType => typeof(T);
    public Expression Expression { get; }
    public IQueryProvider Provider { get; }

    internal Db2Queryable(IQueryProvider provider)
    {
        ArgumentNullException.ThrowIfNull(provider);

        Provider = provider;
        Expression = Expression.Constant(this);
    }

    public Db2Queryable(IQueryProvider provider, Expression expression)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(expression);

        Provider = provider;
        Expression = expression;
    }

    public IEnumerator<T> GetEnumerator()
        => Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
