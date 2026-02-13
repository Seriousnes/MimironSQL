using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

using MimironSQL.EntityFrameworkCore.Storage;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

/// <summary>
/// Factory that creates <see cref="Db2QueryContext"/> instances for query execution.
/// </summary>
internal sealed class Db2QueryContextFactory(
    QueryContextDependencies dependencies,
    IMimironDb2Store store) : IQueryContextFactory
{
    public QueryContext Create()
        => new Db2QueryContext(dependencies, store);
}
