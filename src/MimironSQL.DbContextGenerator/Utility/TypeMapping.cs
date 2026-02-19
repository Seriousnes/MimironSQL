using System.Globalization;

using MimironSQL.Dbd;
using MimironSQL.Db2;

namespace MimironSQL.DbContextGenerator.Utility;

internal static class TypeMapping
{
    /// <summary>
    /// Gets the CLR type name used for the entity key based on the DBD layout and column metadata.
    /// </summary>
    /// <param name="idEntry">The DBD entry describing the key.</param>
    /// <param name="columnsByName">DBD columns keyed by column name.</param>
    /// <returns>A CLR type name.</returns>
    public static string GetIdClrType(DbdLayoutEntry? idEntry, IReadOnlyDictionary<string, DbdColumn> columnsByName)
    {
        if (idEntry is null)
            return "int";

        if (idEntry.Name is null)
            return "int";

        if (TryMapInlineInteger(idEntry.InlineTypeToken, out var mapped))
            return PromoteUnsignedKeyType(mapped);

        if (columnsByName.TryGetValue(idEntry.Name, out var col))
        {
            return col.ValueType switch
            {
                Db2ValueType.UInt64 => "uint",
                Db2ValueType.Int64 => "int",
                Db2ValueType.String or Db2ValueType.LocString => "string",
                _ => "int",
            };
        }

        return "int";
    }

    private static string PromoteUnsignedKeyType(string typeName)
    {
        return typeName switch
        {
            "byte" => "short",
            "ushort" => "int",
            "uint" => "long",
            _ => typeName,
        };
    }

    /// <summary>
    /// Gets the CLR type name for the specified DBD entry.
    /// </summary>
    /// <param name="entry">The DBD entry.</param>
    /// <returns>A CLR type name.</returns>
    public static string GetClrTypeName(DbdLayoutEntry entry)
    {
        var elementType = GetClrElementTypeName(entry);
        return entry.ElementCount > 1 ? $"{elementType}[]" : elementType;
    }

    private static string GetClrElementTypeName(DbdLayoutEntry entry)
    {
        if (TryMapInlineInteger(entry.InlineTypeToken, out var mapped))
            return mapped;

        return entry.ValueType switch
        {
            Db2ValueType.Single => "float",
            Db2ValueType.String or Db2ValueType.LocString => "string",
            Db2ValueType.UInt64 => "uint",
            Db2ValueType.Int64 => "int",
            _ => "int",
        };
    }

    /// <summary>
    /// Gets the initializer source text for a generated property of the given type.
    /// </summary>
    /// <param name="typeName">The CLR type name.</param>
    /// <returns>An initializer string, or an empty string when no initializer is required.</returns>
    public static string GetInitializer(string typeName)
    {
        if (typeName.EndsWith("[]", StringComparison.Ordinal))
            return " = [];";

        if (typeName.StartsWith("ICollection<", StringComparison.Ordinal))
            return " = [];";

        return typeName switch
        {
            "string" => " = string.Empty;",
            _ => string.Empty,
        };
    }

    public static bool TryMapInlineInteger(string? inlineTypeToken, out string clrType)
    {
        clrType = string.Empty;

        if (inlineTypeToken is null)
            return false;

        var token = inlineTypeToken.Trim();
        if (token.Length == 0)
            return false;

        var isUnsigned = token.StartsWith("u", StringComparison.Ordinal);
        var numberText = isUnsigned ? token.Substring(1) : token;

        if (!int.TryParse(numberText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var bits))
            return false;

        clrType = (bits, isUnsigned) switch
        {
            (8, true) => "byte",
            (8, false) => "sbyte",
            (16, true) => "ushort",
            (16, false) => "short",
            (32, true) => "uint",
            (32, false) => "int",
            (64, true) => "ulong",
            (64, false) => "long",
            _ => "int",
        };

        return true;
    }
}