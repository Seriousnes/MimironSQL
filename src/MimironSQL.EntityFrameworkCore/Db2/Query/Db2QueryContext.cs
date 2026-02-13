using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

using MimironSQL.EntityFrameworkCore.Storage;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

/// <summary>
/// DB2-specific query context that provides access to the store at runtime.
/// </summary>
internal sealed class Db2QueryContext(
    QueryContextDependencies dependencies,
    IMimironDb2Store store) : QueryContext(dependencies)
{
    /// <summary>
    /// The DB2 store for opening tables and reading data.
    /// </summary>
    public IMimironDb2Store Store { get; } = store;
}
