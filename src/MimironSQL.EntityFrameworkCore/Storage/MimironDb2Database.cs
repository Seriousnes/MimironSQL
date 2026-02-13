using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

using MimironSQL.EntityFrameworkCore.Query.Internal;

namespace MimironSQL.EntityFrameworkCore.Storage;

#pragma warning disable EF1001 // Internal EF Core API usage is isolated to this provider.
internal sealed class MimironDb2Database : IDatabase
{
    public int SaveChanges(IList<IUpdateEntry> entries)
        => throw new NotSupportedException("MimironDB2 is a read-only provider; SaveChanges is not supported.");

    public Task<int> SaveChangesAsync(IList<IUpdateEntry> entries, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("MimironDB2 is a read-only provider; SaveChangesAsync is not supported.");

    public Func<QueryContext, TResult> CompileQuery<TResult>(Expression query, bool async)
    {
        // Resolve the executor per-execution from the current DbContext scope.
        // EF Core caches compiled query delegates in CompiledQueryCache (singleton),
        // so captured scoped services would become stale across DbContext instances.
        if (async)
            return ctx =>
            {
                var queryCompiler = ctx.Context.GetService<IQueryCompiler>();
                return MimironDb2AsyncQueryAdapter.ExecuteAsync<TResult>(queryCompiler, query);
            };

        return ctx =>
        {
            var queryCompiler = ctx.Context.GetService<IQueryCompiler>();
            return queryCompiler.Execute<TResult>(query);
        };
    }

    public Expression<Func<QueryContext, TResult>> CompileQueryExpression<TResult>(Expression query, bool async)
    {
        // Build an expression tree that resolves the executor per-execution
        // instead of capturing a scoped instance.
        var qc = Expression.Parameter(typeof(QueryContext), "qc");

        var contextProp = Expression.Property(qc, nameof(QueryContext.Context));
        var getService = typeof(AccessorExtensions)
            .GetMethod(nameof(AccessorExtensions.GetService))!
            .MakeGenericMethod(typeof(IQueryCompiler));
        var queryCompilerExpr = Expression.Call(getService, contextProp);

        if (!async)
        {
            var execute = Expression.Call(
                instance: queryCompilerExpr,
                method: typeof(IQueryCompiler).GetMethod(nameof(IQueryCompiler.Execute))!.MakeGenericMethod(typeof(TResult)),
                arguments: Expression.Constant(query));

            return Expression.Lambda<Func<QueryContext, TResult>>(execute, qc);
        }

        var asyncCall = Expression.Call(
            typeof(MimironDb2AsyncQueryAdapter).GetMethod(nameof(MimironDb2AsyncQueryAdapter.ExecuteAsync), [typeof(IQueryCompiler), typeof(Expression)])!
                .MakeGenericMethod(typeof(TResult)),
            queryCompilerExpr,
            Expression.Constant(query));

        return Expression.Lambda<Func<QueryContext, TResult>>(asyncCall, qc);
    }
}

#pragma warning restore EF1001
