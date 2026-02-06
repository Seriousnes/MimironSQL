using Microsoft.EntityFrameworkCore.Query;
using MimironSQL.Db2.Model;
using MimironSQL.Db2.Query;
using MimironSQL.Db2.Schema;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;
using System.Linq.Expressions;

namespace MimironSQL.EntityFrameworkCore.Query;

internal sealed class MimironDb2QueryProvider<TEntity, TRow> : IAsyncQueryProvider
    where TRow : struct, IRowHandle
{
    private readonly Db2QueryProvider<TEntity, TRow> _innerProvider;

    public MimironDb2QueryProvider(
        IDb2File<TRow> file,
        Db2Model model,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver)
    {
        _innerProvider = new Db2QueryProvider<TEntity, TRow>(file, model, tableResolver);
    }

    public IQueryable CreateQuery(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        
        var innerQueryable = _innerProvider.CreateQuery(expression);
        return new MimironDb2Queryable<object>(this, innerQueryable.Expression);
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        return new MimironDb2Queryable<TElement>(this, expression);
    }

    public object? Execute(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        return _innerProvider.Execute(expression);
    }

    public TResult Execute<TResult>(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        return _innerProvider.Execute<TResult>(expression);
    }

    public TResult ExecuteAsync<TResult>(Expression expression, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(expression);
        cancellationToken.ThrowIfCancellationRequested();

        return _innerProvider.Execute<TResult>(expression);
    }
}
