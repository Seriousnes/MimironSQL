using Microsoft.EntityFrameworkCore.Query;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

internal sealed class MimironDb2QueryContext(QueryContextDependencies dependencies) : QueryContext(dependencies);
