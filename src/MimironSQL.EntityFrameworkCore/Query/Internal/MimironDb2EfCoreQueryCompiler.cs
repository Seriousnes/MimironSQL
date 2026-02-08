using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

#pragma warning disable EF1001 // Internal EF Core API usage is isolated to this shim.
internal sealed class MimironDb2EfCoreQueryCompiler(IMimironDb2QueryExecutor executor) : IQueryCompiler
{
    private readonly IMimironDb2QueryExecutor _executor = executor ?? throw new ArgumentNullException(nameof(executor));

    public TResult Execute<TResult>(Expression query)
        => _executor.Execute<TResult>(query);

    public Func<QueryContext, TResult> CreateCompiledQuery<TResult>(Expression query)
        => _ => _executor.Execute<TResult>(query);

    public Func<QueryContext, TResult> CreateCompiledAsyncQuery<TResult>(Expression query)
        => _ => throw new NotSupportedException("Async query execution is not supported.");

    public TResult ExecuteAsync<TResult>(Expression query, CancellationToken cancellationToken)
        => throw new NotSupportedException("Async query execution is not supported.");

    public Expression<Func<QueryContext, TResult>> PrecompileQuery<TResult>(Expression query, bool async)
        => throw new NotSupportedException("Query precompilation is not supported.");
}
#pragma warning restore EF1001
