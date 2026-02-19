using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore.Metadata;

namespace MimironSQL.EntityFrameworkCore.Db2.Query.Visitor;

internal sealed class JoinValueBufferIndexOffsetVisitor(IReadOnlyDictionary<IPropertyBase, int> propertyOffsets) : ExpressionVisitor
{
    private readonly IReadOnlyDictionary<IPropertyBase, int> _propertyOffsets = propertyOffsets;

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if (node.Method.Name != "ValueBufferTryReadValue" || node.Arguments.Count < 3)
            return base.VisitMethodCall(node);

        if (node.Arguments[1] is not ConstantExpression { Type: { } t, Value: int index } || t != typeof(int))
            return base.VisitMethodCall(node);

        if (node.Arguments[2] is not ConstantExpression { Value: IPropertyBase propertyBase })
            return base.VisitMethodCall(node);

        if (!_propertyOffsets.TryGetValue(propertyBase, out var offset) || offset == 0)
            return base.VisitMethodCall(node);

        var visitedObject = Visit(node.Object);
        var newArgs = new Expression[node.Arguments.Count];
        for (var i = 0; i < node.Arguments.Count; i++)
        {
            newArgs[i] = i == 1
                ? Expression.Constant(index + offset)
                : Visit(node.Arguments[i]);
        }

        return Expression.Call(visitedObject, node.Method, newArgs);
    }
}
