using Microsoft.EntityFrameworkCore.Query;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

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
