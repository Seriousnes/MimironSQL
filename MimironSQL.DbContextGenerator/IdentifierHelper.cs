using System.Text;

namespace MimironSQL.DbContextGenerator;

internal static class IdentifierHelper
{
    private static readonly HashSet<string> Keywords = new(StringComparer.Ordinal)
    {
        "abstract", "as", "base", "bool", "break", "byte", "case", "catch", "char", "checked",
        "class", "const", "continue", "decimal", "default", "delegate", "do", "double", "else",
        "enum", "event", "explicit", "extern", "false", "finally", "fixed", "float", "for",
        "foreach", "goto", "if", "implicit", "in", "int", "interface", "internal", "is",
        "lock", "long", "namespace", "new", "null", "object", "operator", "out", "override",
        "params", "private", "protected", "public", "readonly", "ref", "return", "sbyte",
        "sealed", "short", "sizeof", "stackalloc", "static", "string", "struct", "switch",
        "this", "throw", "true", "try", "typeof", "uint", "ulong", "unchecked", "unsafe",
        "ushort", "using", "virtual", "void", "volatile", "while"
    };

    public static string SanitizeNamespacePart(string value)
        => SanitizeLoosePart(value);

    public static string SanitizeLoosePart(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return "Unknown";

        var sb = new StringBuilder();
        foreach (var c in value)
            sb.Append(char.IsLetterOrDigit(c) ? c : '_');

        var candidate = sb.ToString().Trim('_');
        return candidate.Length == 0 ? "Unknown" : candidate;
    }

    public static string SanitizeIdentifierPart(string value)
    {
        var candidate = SanitizeLoosePart(value);

        // Don't start an identifier with a digit.
        if (char.IsDigit(candidate[0]))
            candidate = "_" + candidate;

        return candidate;
    }

    public static string ToSafeTypeName(string name)
        => ToSafeIdentifier(ToPascalCase(name));

    public static string ToSafeMemberName(string name)
        => ToSafeIdentifier(ToPascalCase(name));

    public static string ToSafeIdentifier(string name)
    {
        var sanitized = SanitizeIdentifierPart(name);
        return Keywords.Contains(sanitized) ? "@" + sanitized : sanitized;
    }

    private static string ToPascalCase(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return "Unknown";

        var parts = name.Split(['_', '-', ' '], StringSplitOptions.RemoveEmptyEntries);
        var sb = new StringBuilder();
        foreach (var p in parts)
        {
            if (p.Length == 0)
                continue;

            sb.Append(char.ToUpperInvariant(p[0]));
            if (p.Length > 1)
                sb.Append(p.Substring(1));
        }

        return sb.Length == 0 ? "Unknown" : sb.ToString();
    }
}
