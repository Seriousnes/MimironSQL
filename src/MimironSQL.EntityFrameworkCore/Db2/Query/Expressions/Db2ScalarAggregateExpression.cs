using System.Linq.Expressions;

namespace MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;

internal enum Db2ScalarAggregateKind
{
    Any,
    Count,
}

/// <summary>
/// Marker expression used as the shaper for scalar terminal operators (e.g. Any/Count).
/// The actual evaluation is performed at query compilation time by the provider.
/// </summary>
internal sealed class Db2ScalarAggregateExpression(Db2ScalarAggregateKind kind, bool negate) : Expression
{
    public Db2ScalarAggregateKind Kind { get; } = kind;
    public bool IsNegated { get; } = negate;

    public override Type Type => Kind switch
    {
        Db2ScalarAggregateKind.Any => typeof(bool),
        Db2ScalarAggregateKind.Count => typeof(int),
        _ => typeof(object),
    };

    public override ExpressionType NodeType => ExpressionType.Extension;

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override string ToString() => IsNegated ? $"NOT {Kind}()" : $"{Kind}()";
}
