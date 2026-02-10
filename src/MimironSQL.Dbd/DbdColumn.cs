using MimironSQL.Db2;

using System.Diagnostics.CodeAnalysis;

namespace MimironSQL.Dbd;

/// <summary>
/// Represents a column definition from the COLUMNS section of a DBD file.
/// </summary>
public sealed class DbdColumn : IDbdColumn
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DbdColumn"/> class.
    /// </summary>
    public DbdColumn(Db2ValueType valueType, string? referencedTableName, bool isVerified)
    {
        ValueType = valueType;
        ReferencedTableName = referencedTableName;
        IsVerified = isVerified;
    }

    /// <inheritdoc />
    public Db2ValueType ValueType { get; }

    /// <inheritdoc />
    public string? ReferencedTableName { get; }

    /// <inheritdoc />
    public bool IsVerified { get; }
}

/// <summary>
/// Parses column declarations in the COLUMNS section of a DBD file.
/// </summary>
public static class DbdColumnParser
{
    /// <summary>
    /// Attempts to parse a column declaration line.
    /// </summary>
    public static bool TryParse(string line, out string name, out DbdColumn? column)
    {
        var text = line.AsSpan();
        var space = text.IndexOf(' ');
        if (space <= 0 || space == text.Length - 1)
        {
            name = string.Empty;
            column = default;
            return false;
        }

        var typeToken = text.Slice(0, space).Trim();
        var nameSpan = text.Slice(space + 1).Trim();
        var isVerified = true;
        if (nameSpan.EndsWith("?".AsSpan(), StringComparison.Ordinal))
        {
            nameSpan = nameSpan.Slice(0, nameSpan.Length - 1);
            isVerified = false;
        }
        if (nameSpan is { Length: 0 })
        {
            name = string.Empty;
            column = default;
            return false;
        }

        name = nameSpan.ToString();

        var parsed = ParseColumnType(typeToken);
        column = new DbdColumn(parsed.ValueType, parsed.ReferencedTableName, isVerified);
        return true;
    }

    private static DbdColumn ParseColumnType(ReadOnlySpan<char> token)
    {
        var lt = token.IndexOf('<');
        if (lt >= 0)
        {
            var gt = token.LastIndexOf('>');
            if (gt > lt)
            {
                var inner = token.Slice(lt + 1, gt - lt - 1);
                var refName = TryParseReferenceTableName(inner);
                return new DbdColumn(MapTypeToken(token.Slice(0, lt)), refName, isVerified: true);
            }
        }

        return new DbdColumn(MapTypeToken(token), referencedTableName: null, isVerified: true);
    }

    private static string? TryParseReferenceTableName(ReadOnlySpan<char> inner)
    {
        var idx = inner.IndexOf("::".AsSpan(), StringComparison.Ordinal);
        if (idx <= 0)
            return null;

        var table = inner.Slice(0, idx).Trim();
        return table is { Length: 0 } ? null : table.ToString();
    }

    private static Db2ValueType MapTypeToken(ReadOnlySpan<char> token)
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
