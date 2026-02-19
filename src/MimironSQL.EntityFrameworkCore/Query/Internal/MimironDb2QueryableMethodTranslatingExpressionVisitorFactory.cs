using Microsoft.EntityFrameworkCore.Query;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

internal sealed class MimironDb2QueryableMethodTranslatingExpressionVisitorFactory(
    QueryableMethodTranslatingExpressionVisitorDependencies dependencies)
    : IQueryableMethodTranslatingExpressionVisitorFactory
{
    public QueryableMethodTranslatingExpressionVisitor Create(QueryCompilationContext queryCompilationContext)
    {
        ArgumentNullException.ThrowIfNull(queryCompilationContext);
        return new MimironDb2QueryableMethodTranslatingExpressionVisitor(dependencies, queryCompilationContext, subquery: false);
    }
}
