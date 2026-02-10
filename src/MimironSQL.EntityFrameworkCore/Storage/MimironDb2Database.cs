using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

using MimironSQL.EntityFrameworkCore.Query;
using MimironSQL.EntityFrameworkCore.Query.Internal;

namespace MimironSQL.EntityFrameworkCore.Storage;

internal sealed class MimironDb2Database(IMimironDb2QueryExecutor queryExecutor) : IDatabase
{
    private readonly IMimironDb2QueryExecutor _queryExecutor = queryExecutor ?? throw new ArgumentNullException(nameof(queryExecutor));

    public int SaveChanges(IList<IUpdateEntry> entries)
        => throw new NotSupportedException("MimironDB2 is a read-only provider; SaveChanges is not supported.");

    public Task<int> SaveChangesAsync(IList<IUpdateEntry> entries, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("MimironDB2 is a read-only provider; SaveChangesAsync is not supported.");

    public Func<QueryContext, TResult> CompileQuery<TResult>(Expression query, bool async)
    {
        if (async)
            return _ => MimironDb2AsyncQueryAdapter.ExecuteAsync<TResult>(_queryExecutor, query);

        return _ => _queryExecutor.Execute<TResult>(query);
    }

    public Expression<Func<QueryContext, TResult>> CompileQueryExpression<TResult>(Expression query, bool async)
        => MimironDb2AsyncQueryAdapter.PrecompileQuery<TResult>(_queryExecutor, query, async);
}
