using System.Linq.Expressions;

namespace MimironSQL.EntityFrameworkCore.Query.Internal.Visitor;

internal sealed class EntityQueryRootSearchVisitor : ExpressionVisitor
{
    private bool _found;

    public static bool ContainsEntityQueryRoot(Expression expression)
    {
        var v = new EntityQueryRootSearchVisitor();
        v.Visit(expression);
        return v._found;
    }

    protected override Expression VisitExtension(Expression node)
    {
        if (node.GetType().Name == "EntityQueryRootExpression")
            _found = true;

        return base.VisitExtension(node);
    }
}
