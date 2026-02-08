using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

namespace MimironSQL.EntityFrameworkCore.Storage;

#pragma warning disable EF1001 // Internal EF Core API usage is intentional for provider implementation.
internal sealed class MimironDb2Database(IQueryCompiler queryCompiler) : IDatabase
#pragma warning restore EF1001
{
    private readonly IQueryCompiler _queryCompiler = queryCompiler ?? throw new ArgumentNullException(nameof(queryCompiler));

    public int SaveChanges(IList<IUpdateEntry> entries)
        => throw new NotSupportedException("MimironDB2 is a read-only provider; SaveChanges is not supported.");

    public Task<int> SaveChangesAsync(IList<IUpdateEntry> entries, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("MimironDB2 is a read-only provider; SaveChangesAsync is not supported.");

    public Func<QueryContext, TResult> CompileQuery<TResult>(Expression query, bool async)
    {
        if (async)
            return _ => throw new NotSupportedException("Async query execution is not supported.");

        return _ => _queryCompiler.Execute<TResult>(query);
    }

    public Expression<Func<QueryContext, TResult>> CompileQueryExpression<TResult>(Expression query, bool async)
        => throw new NotSupportedException("Query precompilation is not supported.");
}
