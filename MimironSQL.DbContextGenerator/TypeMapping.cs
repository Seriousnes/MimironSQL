namespace CASC.Net.Generators;

internal static class TypeMapping
{
    public static string GetCSharpType(ColumnSpec column)
    {
        var elementType = GetCSharpElementType(column.DbdType);
        return column.ArrayLength is { } ? elementType + "[]" : elementType;
    }

    public static string GetCSharpElementType(string dbdType)
    {
        var normalized = NormalizeDbdType(dbdType);
        return normalized switch
        {
            "int" or "int32" => "int",
            "uint" or "uint32" => "uint",
            "short" or "int16" => "short",
            "ushort" or "uint16" => "ushort",
            "byte" or "uint8" => "byte",
            "sbyte" or "int8" => "sbyte",
            "long" or "int64" => "long",
            "ulong" or "uint64" => "ulong",
            "float" => "float",
            "double" => "double",
            "bool" => "bool",
            "string" or "cstring" or "locstring" => "string",
            _ => "int"
        };
    }

    public static bool IsStringElementType(string dbdType)
        => GetCSharpElementType(dbdType) == "string";

    private static string NormalizeDbdType(string dbdType)
    {
        if (string.IsNullOrWhiteSpace(dbdType))
            return string.Empty;

        var type = dbdType.Trim();
        var genericIndex = type.IndexOf('<');
        if (genericIndex >= 0)
            type = type.Substring(0, genericIndex);

        return type.Trim().ToLowerInvariant();
    }
}
