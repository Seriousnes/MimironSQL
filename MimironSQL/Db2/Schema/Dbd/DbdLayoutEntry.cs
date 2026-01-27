using System.Globalization;

namespace MimironSQL.Db2.Schema.Dbd;

internal readonly record struct DbdLayoutEntry(
    string Name,
    Db2ValueType ValueType,
    string? ReferencedTableName,
    int ElementCount,
    bool IsVerified,
    bool IsNonInline,
    bool IsId,
    bool IsRelation);

internal static class DbdLayoutEntryParser
{
    public static bool TryParse(string line, IReadOnlyDictionary<string, DbdColumn> columnsByName, out DbdLayoutEntry entry)
    {
        var text = line.Trim();
        if (text is { Length: 0 })
        {
            entry = default;
            return false;
        }

        var isNonInline = false;
        var isId = false;
        var isRelation = false;

        if (text[0] is '$')
        {
            var second = text.IndexOf('$', 1);
            if (second > 1)
            {
                var modifiers = text[1..second].Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
                foreach (var m in modifiers)
                {
                    if (m.Equals("noninline", StringComparison.OrdinalIgnoreCase))
                        isNonInline = true;
                    if (m.Equals("id", StringComparison.OrdinalIgnoreCase))
                        isId = true;
                    if (m.Equals("relation", StringComparison.OrdinalIgnoreCase))
                        isRelation = true;
                }

                text = text[(second + 1)..].Trim();
            }
        }

        var elementCount = 1;
        var bracket = text.LastIndexOf('[');
        if (bracket >= 0 && text.EndsWith("]", StringComparison.Ordinal))
        {
            var number = text[(bracket + 1)..^1];
            elementCount = int.Parse(number, NumberStyles.Integer, CultureInfo.InvariantCulture);
            text = text[..bracket];
        }

        string name;
        Db2ValueType valueType;
        var referencedTableName = (string?)null;
        var isVerified = true;

        var lt = text.IndexOf('<');
        if (lt >= 0)
        {
            var gt = text.LastIndexOf('>');
            if (gt <= lt)
            {
                entry = default;
                return false;
            }

            name = text[..lt].Trim();
            var typeInner = text[(lt + 1)..gt].Trim();
            valueType = MapInlineType(typeInner);
        }
        else
        {
            name = text.Trim();
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
        }

        if (name is { Length: 0 })
        {
            entry = default;
            return false;
        }

        if (referencedTableName is null && columnsByName.TryGetValue(name, out var col))
        {
            referencedTableName = col.ReferencedTableName;
            isVerified = col.IsVerified;
        }

        entry = new DbdLayoutEntry(name, valueType, referencedTableName, elementCount, isVerified, isNonInline, isId, isRelation);
        return true;
    }

    private static Db2ValueType MapInlineType(string inner)
    {
        if (inner.StartsWith('u'))
            return Db2ValueType.UInt64;

        if (inner.StartsWith('f'))
            return Db2ValueType.Single;

        if (inner is "8" or "16" or "32" or "64")
            return Db2ValueType.Int64;

        if (inner is "u8" or "u16" or "u32" or "u64")
            return Db2ValueType.UInt64;

        return Db2ValueType.Int64;
    }
}
