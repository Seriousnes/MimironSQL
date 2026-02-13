using Microsoft.EntityFrameworkCore.Query;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

/// <summary>
/// Factory that creates <see cref="Db2QueryableMethodTranslatingExpressionVisitor"/> instances.
/// </summary>
internal sealed class Db2QueryableMethodTranslatingExpressionVisitorFactory(
    QueryableMethodTranslatingExpressionVisitorDependencies dependencies)
    : IQueryableMethodTranslatingExpressionVisitorFactory
{
    private readonly QueryableMethodTranslatingExpressionVisitorDependencies _dependencies = dependencies;

    public QueryableMethodTranslatingExpressionVisitor Create(QueryCompilationContext queryCompilationContext)
        => new Db2QueryableMethodTranslatingExpressionVisitor(_dependencies, queryCompilationContext);
}
