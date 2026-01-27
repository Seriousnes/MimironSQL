namespace MimironSQL.Db2.Schema.Dbd;

internal readonly record struct DbdColumn(Db2ValueType ValueType, string? ReferencedTableName, bool IsVerified);

internal static class DbdColumnParser
{
    public static bool TryParse(string line, out string name, out DbdColumn column)
    {
        // Format: "type name" (e.g., "locstring MapName_lang", "int<Map::ID> ParentMapID")
        var space = line.IndexOf(' ');
        if (space <= 0 || space == line.Length - 1)
        {
            name = string.Empty;
            column = default;
            return false;
        }

        var typeToken = line[..space].Trim();
        name = line[(space + 1)..].Trim();
        var isVerified = true;
        if (name.EndsWith("?", StringComparison.Ordinal))
        {
            name = name[..^1];
            isVerified = false;
        }
        if (name is { Length: 0 })
        {
            column = default;
            return false;
        }

        column = ParseColumnType(typeToken) with { IsVerified = isVerified };
        return true;
    }

    private static DbdColumn ParseColumnType(string token)
    {
        // Examples:
        // - int
        // - int<Map::ID>
        // - int<ActionBarGroup::ID>
        var lt = token.IndexOf('<');
        if (lt >= 0)
        {
            var gt = token.LastIndexOf('>');
            if (gt > lt)
            {
                var inner = token[(lt + 1)..gt];
                var refName = TryParseReferenceTableName(inner);
                return new DbdColumn(MapTypeToken(token[..lt]), refName, IsVerified: true);
            }
        }

        return new DbdColumn(MapTypeToken(token), ReferencedTableName: null, IsVerified: true);
    }

    private static string? TryParseReferenceTableName(string inner)
    {
        // Inner is usually like "Map::ID" or "QuestV2::ID"
        var idx = inner.IndexOf("::", StringComparison.Ordinal);
        if (idx <= 0)
            return null;

        var table = inner[..idx].Trim();
        return table is { Length: 0 } ? null : table;
    }

    private static Db2ValueType MapTypeToken(string token)
    {
        if (token.StartsWith("int", StringComparison.Ordinal))
            return Db2ValueType.Int64;
        if (token.StartsWith("uint", StringComparison.Ordinal))
            return Db2ValueType.UInt64;
        if (token.StartsWith("float", StringComparison.Ordinal))
            return Db2ValueType.Single;
        if (token.StartsWith("string", StringComparison.Ordinal))
            return Db2ValueType.String;
        if (token.StartsWith("locstring", StringComparison.Ordinal))
            return Db2ValueType.LocString;

        return Db2ValueType.Unknown;
    }
}
