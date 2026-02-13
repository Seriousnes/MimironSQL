using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

namespace MimironSQL.EntityFrameworkCore.Storage;

/// <summary>
/// EF Core <see cref="IDatabase"/> implementation for the MimironDB2 provider.
/// Uses the standard EF Core query compilation pipeline via <see cref="IQueryCompilationContextFactory"/>.
/// </summary>
internal sealed class MimironDb2Database(DatabaseDependencies dependencies) : Database(dependencies)
{
    public override int SaveChanges(IList<IUpdateEntry> entries)
        => throw new NotSupportedException("MimironDB2 is a read-only provider; SaveChanges is not supported.");

    public override Task<int> SaveChangesAsync(IList<IUpdateEntry> entries, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("MimironDB2 is a read-only provider; SaveChangesAsync is not supported.");

    // CompileQuery and CompileQueryExpression are inherited from Database base class,
    // which uses QueryCompilationContextFactory.Create(async).CreateQueryExecutor<TResult>(query).
}
