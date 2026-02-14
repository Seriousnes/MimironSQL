using Microsoft.EntityFrameworkCore.Query;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

internal sealed class MimironDb2QueryContextFactory(QueryContextDependencies dependencies) : IQueryContextFactory
{
    public QueryContext Create() => new MimironDb2QueryContext(dependencies);
}
