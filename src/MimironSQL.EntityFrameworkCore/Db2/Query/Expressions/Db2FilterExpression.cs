using System.Linq.Expressions;

using MimironSQL.EntityFrameworkCore.Db2.Query;

namespace MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;

/// <summary>
/// Base class for all DB2 filter expression nodes.
/// </summary>
internal abstract class Db2FilterExpression : Expression
{
    public override Type Type => typeof(bool);
    public sealed override ExpressionType NodeType => ExpressionType.Extension;
}

/// <summary>
/// Represents a field comparison filter: field op value.
/// </summary>
internal sealed class Db2ComparisonFilterExpression(
    Db2FieldAccessExpression field,
    ExpressionType comparisonKind,
    object? value) : Db2FilterExpression
{
    public new Db2FieldAccessExpression Field { get; } = field;
    public ExpressionType ComparisonKind { get; } = comparisonKind;
    public object? Value { get; } = value;

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override string ToString() => $"{Field} {ComparisonKind} {Value}";
}

/// <summary>
/// Represents a field IN (values) filter. Values can be concrete or resolved from a runtime parameter.
/// </summary>
internal sealed class Db2ContainsFilterExpression : Db2FilterExpression
{
    public new Db2FieldAccessExpression Field { get; }

    /// <summary>Concrete values for the IN list (null when <see cref="ValuesParameterName"/> is set).</summary>
    public IReadOnlyList<object>? Values { get; }

    /// <summary>Runtime parameter name whose value is a collection (null when <see cref="Values"/> is set).</summary>
    public string? ValuesParameterName { get; }

    /// <summary>Creates a Contains filter with concrete values.</summary>
    public Db2ContainsFilterExpression(Db2FieldAccessExpression field, IReadOnlyList<object> values)
    {
        Field = field;
        Values = values;
    }

    /// <summary>Creates a Contains filter whose values come from a runtime parameter.</summary>
    public Db2ContainsFilterExpression(Db2FieldAccessExpression field, string valuesParameterName)
    {
        Field = field;
        ValuesParameterName = valuesParameterName;
    }

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override string ToString() => ValuesParameterName is not null
        ? $"{Field} IN (@{ValuesParameterName})"
        : $"{Field} IN ({string.Join(", ", Values!)})";
}

/// <summary>
/// Represents a string match filter: Contains, StartsWith, EndsWith.
/// </summary>
internal sealed class Db2StringMatchFilterExpression : Db2FilterExpression
{
    public new Db2FieldAccessExpression Field { get; }
    public Db2StringMatchKind MatchKind { get; }

    /// <summary>Concrete pattern string (null when <see cref="PatternParameterName"/> is set).</summary>
    public string? Pattern { get; }

    /// <summary>Runtime parameter name for the pattern string (null when <see cref="Pattern"/> is set).</summary>
    public string? PatternParameterName { get; }

    public Db2StringMatchFilterExpression(Db2FieldAccessExpression field, Db2StringMatchKind matchKind, string pattern)
    {
        Field = field;
        MatchKind = matchKind;
        Pattern = pattern;
    }

    public Db2StringMatchFilterExpression(Db2FieldAccessExpression field, Db2StringMatchKind matchKind, string patternParameterName, bool isParameter)
    {
        _ = isParameter;
        Field = field;
        MatchKind = matchKind;
        PatternParameterName = patternParameterName;
    }

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override string ToString() => PatternParameterName is not null
        ? $"{Field}.{MatchKind}(@{PatternParameterName})"
        : $"{Field}.{MatchKind}(\"{Pattern}\")";
}

/// <summary>
/// Represents a logical AND of two filter expressions.
/// </summary>
internal sealed class Db2AndFilterExpression(
    Db2FilterExpression left,
    Db2FilterExpression right) : Db2FilterExpression
{
    public Db2FilterExpression Left { get; } = left;
    public Db2FilterExpression Right { get; } = right;

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override string ToString() => $"({Left} AND {Right})";
}

/// <summary>
/// Represents a logical OR of two filter expressions.
/// </summary>
internal sealed class Db2OrFilterExpression(
    Db2FilterExpression left,
    Db2FilterExpression right) : Db2FilterExpression
{
    public Db2FilterExpression Left { get; } = left;
    public Db2FilterExpression Right { get; } = right;

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override string ToString() => $"({Left} OR {Right})";
}

/// <summary>
/// Represents a logical NOT of a filter expression.
/// </summary>
internal sealed class Db2NotFilterExpression(
    Db2FilterExpression inner) : Db2FilterExpression
{
    public Db2FilterExpression Inner { get; } = inner;

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override string ToString() => $"NOT ({Inner})";
}

/// <summary>
/// Represents a null check filter: field IS NULL / IS NOT NULL.
/// </summary>
internal sealed class Db2NullCheckFilterExpression(
    Db2FieldAccessExpression field,
    bool isNotNull) : Db2FilterExpression
{
    public new Db2FieldAccessExpression Field { get; } = field;
    public bool IsNotNull { get; } = isNotNull;

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override string ToString() => IsNotNull ? $"{Field} IS NOT NULL" : $"{Field} IS NULL";
}

/// <summary>
/// Represents a string length comparison filter: field.Length op value.
/// </summary>
internal sealed class Db2StringLengthFilterExpression(
    Db2FieldAccessExpression field,
    ExpressionType comparisonKind,
    int value) : Db2FilterExpression
{
    public new Db2FieldAccessExpression Field { get; } = field;
    public ExpressionType ComparisonKind { get; } = comparisonKind;
    public int Value { get; } = value;

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override string ToString() => $"{Field}.Length {ComparisonKind} {Value}";
}

/// <summary>
/// Represents a comparison filter on a field from a joined (inner) table.
/// Used for patterns like <c>x.Map.Id == 1</c> or <c>x.Map.Directory == "foo"</c>.
/// </summary>
internal sealed class Db2JoinedComparisonFilterExpression(
    Db2JoinedFieldAccessExpression field,
    ExpressionType comparisonKind,
    object? value) : Db2FilterExpression
{
    public new Db2JoinedFieldAccessExpression Field { get; } = field;
    public ExpressionType ComparisonKind { get; } = comparisonKind;
    public object? Value { get; } = value;

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override string ToString() => $"{Field} {ComparisonKind} {Value}";
}

/// <summary>
/// Represents a string match filter on a field from a joined (inner) table.
/// Used for patterns like <c>x.Map.Directory.Contains("foo")</c>.
/// </summary>
internal sealed class Db2JoinedStringMatchFilterExpression : Db2FilterExpression
{
    public new Db2JoinedFieldAccessExpression Field { get; }
    public Db2StringMatchKind MatchKind { get; }

    public string? Pattern { get; }
    public string? PatternParameterName { get; }

    public Db2JoinedStringMatchFilterExpression(Db2JoinedFieldAccessExpression field, Db2StringMatchKind matchKind, string pattern)
    {
        Field = field;
        MatchKind = matchKind;
        Pattern = pattern;
    }

    public Db2JoinedStringMatchFilterExpression(Db2JoinedFieldAccessExpression field, Db2StringMatchKind matchKind, string patternParameterName, bool isParameter)
    {
        _ = isParameter;
        Field = field;
        MatchKind = matchKind;
        PatternParameterName = patternParameterName;
    }

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override string ToString() => PatternParameterName is not null
        ? $"{Field}.{MatchKind}(@{PatternParameterName})"
        : $"{Field}.{MatchKind}(\"{Pattern}\")";
}

/// <summary>
/// Represents a null check on a field from a joined (inner) table.
/// Used for patterns like <c>x.Map != null</c>.
/// </summary>
internal sealed class Db2JoinedNullCheckFilterExpression(
    Db2JoinedFieldAccessExpression field,
    bool isNotNull) : Db2FilterExpression
{
    public new Db2JoinedFieldAccessExpression Field { get; } = field;
    public bool IsNotNull { get; } = isNotNull;

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override string ToString() => IsNotNull ? $"{Field} IS NOT NULL" : $"{Field} IS NULL";
}