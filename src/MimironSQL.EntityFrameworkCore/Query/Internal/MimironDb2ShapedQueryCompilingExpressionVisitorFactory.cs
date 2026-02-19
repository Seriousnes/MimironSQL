using Microsoft.EntityFrameworkCore.Query;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

internal sealed class MimironDb2ShapedQueryCompilingExpressionVisitorFactory(
    ShapedQueryCompilingExpressionVisitorDependencies dependencies)
    : IShapedQueryCompilingExpressionVisitorFactory
{
    public ShapedQueryCompilingExpressionVisitor Create(QueryCompilationContext queryCompilationContext)
    {
        ArgumentNullException.ThrowIfNull(queryCompilationContext);
        return new MimironDb2ShapedQueryCompilingExpressionVisitor(dependencies, queryCompilationContext);
    }
}
