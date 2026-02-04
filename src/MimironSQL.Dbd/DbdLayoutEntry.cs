using System.Globalization;

using MimironSQL.Db2;

namespace MimironSQL.Dbd;

public sealed class DbdLayoutEntry(
    string name,
    Db2ValueType valueType,
    string? referencedTableName,
    int elementCount,
    bool isVerified,
    bool isNonInline,
    bool isId,
    bool isRelation,
    string? inlineTypeToken) : IDbdLayoutEntry
{
    public string Name { get; } = name;
    public Db2ValueType ValueType { get; } = valueType;
    public string? ReferencedTableName { get; } = referencedTableName;
    public int ElementCount { get; } = elementCount;
    public bool IsVerified { get; } = isVerified;
    public bool IsNonInline { get; } = isNonInline;
    public bool IsId { get; } = isId;
    public bool IsRelation { get; } = isRelation;
    public string? InlineTypeToken { get; } = inlineTypeToken;
}

public static class DbdLayoutEntryParser
{
    public static bool TryParse(string line, IReadOnlyDictionary<string, DbdColumn> columnsByName, out DbdLayoutEntry entry)
    {
        var text = line.AsSpan().Trim();
        if (text is { Length: 0 })
        {
            entry = null!;
            return false;
        }

        var isNonInline = false;
        var isId = false;
        var isRelation = false;

        if (text[0] is '$')
        {
            var second = text.Slice(1).IndexOf('$');
            if (second > 1)
            {
                second += 1;
                var modifierText = text.Slice(1, second - 1);
                foreach (var c in modifierText)
                {
                    if (c is ' ' or '\t' or '\r' or '\n')
                    {
                        entry = default;
                        return false;
                    }
                }

                while (modifierText.Length != 0)
                {
                    var comma = modifierText.IndexOf(',');
                    var token = comma >= 0 ? modifierText.Slice(0, comma) : modifierText;
                    if (token.Length != 0)
                    {
                        if (token.Equals("noninline".AsSpan(), StringComparison.OrdinalIgnoreCase))
                            isNonInline = true;
                        if (token.Equals("id".AsSpan(), StringComparison.OrdinalIgnoreCase))
                            isId = true;
                        if (token.Equals("relation".AsSpan(), StringComparison.OrdinalIgnoreCase))
                            isRelation = true;
                    }

                    if (comma < 0)
                        break;

                    modifierText = modifierText.Slice(comma + 1);
                }

                text = text.Slice(second + 1).Trim();
            }
        }

        var elementCount = 1;
        var bracket = text.LastIndexOf('[');
        if (bracket >= 0 && text.EndsWith("]".AsSpan(), StringComparison.Ordinal))
        {
            var number = text.Slice(bracket + 1, text.Length - bracket - 2);
            elementCount = int.Parse(number.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture);
            text = text.Slice(0, bracket);
        }

        string name;
        Db2ValueType valueType;
        var referencedTableName = (string?)null;
        var isVerified = true;
        var inlineTypeToken = (string?)null;

        var lt = text.IndexOf('<');
        switch (lt)
        {
            case >= 0:
                {
                    var gt = text.LastIndexOf('>');
                    if (gt <= lt)
                    {
                        entry = null!;
                        return false;
                    }

                    name = text.Slice(0, lt).Trim().ToString();
                    var typeInner = text.Slice(lt + 1, gt - lt - 1).Trim();
                    inlineTypeToken = typeInner.ToString();
                    valueType = MapInlineType(typeInner);
                    break;
                }

            default:
                {
                    name = text.Trim().ToString();
                    if (columnsByName.TryGetValue(name, out var column))
                    {
                        valueType = column.ValueType;
                        referencedTableName = column.ReferencedTableName;
                        isVerified = column.IsVerified;
                    }
                    else
                    {
                        valueType = Db2ValueType.Unknown;
                    }

                    break;
                }
        }

        if (name is { Length: 0 })
        {
            entry = null!;
            return false;
        }

        if (referencedTableName is null && columnsByName.TryGetValue(name, out var col))
        {
            referencedTableName = col.ReferencedTableName;
            isVerified = col.IsVerified;
        }

        entry = new DbdLayoutEntry(name, valueType, referencedTableName, elementCount, isVerified, isNonInline, isId, isRelation, inlineTypeToken);
        return true;
    }

    private static Db2ValueType MapInlineType(ReadOnlySpan<char> inner)
    {
        if (inner.StartsWith("f", StringComparison.Ordinal))
            return Db2ValueType.Single;

        return inner.StartsWith("u", StringComparison.Ordinal)
            ? Db2ValueType.UInt64
            : Db2ValueType.Int64;
    }
}
