using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Metadata;

namespace MimironSQL.EntityFrameworkCore.Query.Internal.Visitor;

internal readonly record struct IncludePlan(Type SourceClrType, INavigationBase Navigation);

internal sealed class IncludeExpressionExtractingVisitor : ExpressionVisitor
{
    private readonly List<IncludePlan> _includePlans = [];

    public IReadOnlyList<IncludePlan> IncludePlans => _includePlans;

    protected override Expression VisitExtension(Expression node)
    {
        if (node is IncludeExpression includeExpression)
        {
            var entityExpression = Visit(includeExpression.EntityExpression);

            // ThenInclude paths can be nested under the navigation expression.
            // We visit it to discover and extract nested IncludeExpression nodes.
            _ = Visit(includeExpression.NavigationExpression);

            if (includeExpression.Navigation is not INavigationBase navigation)
                throw new NotSupportedException("MimironDb2 IncludeExpression without INavigationBase navigation is not supported.");

            // For ThenInclude, EntityExpression can be a collection type (e.g. IEnumerable<T>), which
            // would prevent matching materialized entity instances when executing includes.
            // Use the navigation's declaring entity CLR type as the source type.
            _includePlans.Add(new IncludePlan(navigation.DeclaringEntityType.ClrType, navigation));

            // IncludeExpression returns the entity.
            return entityExpression;
        }

        return base.VisitExtension(node);
    }
}
