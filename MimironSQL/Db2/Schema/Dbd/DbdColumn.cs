using System;

namespace MimironSQL.Db2.Schema.Dbd;

internal readonly record struct DbdColumn(Db2ValueType ValueType);

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
        if (name.Length == 0)
        {
            column = default;
            return false;
        }

        column = new DbdColumn(MapTypeToken(typeToken));
        return true;
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
