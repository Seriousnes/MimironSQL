using Microsoft.EntityFrameworkCore.Query;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

/// <summary>
/// Factory that creates <see cref="Db2ShapedQueryCompilingExpressionVisitor"/> instances.
/// </summary>
internal sealed class Db2ShapedQueryCompilingExpressionVisitorFactory(
    ShapedQueryCompilingExpressionVisitorDependencies dependencies)
    : IShapedQueryCompilingExpressionVisitorFactory
{
    private readonly ShapedQueryCompilingExpressionVisitorDependencies _dependencies = dependencies;

    public ShapedQueryCompilingExpressionVisitor Create(QueryCompilationContext queryCompilationContext)
        => new Db2ShapedQueryCompilingExpressionVisitor(_dependencies, queryCompilationContext);
}
