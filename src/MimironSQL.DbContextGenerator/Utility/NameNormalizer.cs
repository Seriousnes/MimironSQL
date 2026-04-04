using System.Globalization;
using System.Text;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace MimironSQL.DbContextGenerator.Utility;

internal static class NameNormalizer
{
    /// <summary>
    /// Normalizes a DBD table name into a CLR type name.
    /// </summary>
    /// <param name="tableName">The source table name.</param>
    /// <returns>A normalized CLR type name.</returns>
    public static string NormalizeTypeName(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            return "_";
        }

        // Preserve current behavior for already-CLR-safe names that don't need normalization.
        // If the name has underscores, we historically PascalCased it.
        // If the name has hyphens or other non-identifier characters, we normalize/escape to a stable identifier.
        var hasUnderscore = tableName.IndexOf('_') >= 0;
        var needsEscaping = tableName.IndexOfAny(['-', '.', ' ', '\t', '\r', '\n']) >= 0
            || tableName.Any(static c => !(char.IsLetterOrDigit(c) || c == '_'))
            || char.IsDigit(tableName[0]);

        if (!needsEscaping)
        {
            return hasUnderscore
                ? EscapeIdentifier(ToPascalCase(tableName))
                : EscapeIdentifier(tableName);
        }

        return EscapeIdentifier(NormalizeTypeNameWithEscapes(tableName));
    }

    private static string NormalizeTypeNameWithEscapes(string tableName)
    {
        var sb = new StringBuilder(tableName.Length + 8);
        var token = new StringBuilder(capacity: 16);

        void FlushToken()
        {
            if (token.Length == 0)
            {
                return;
            }

            sb.Append(char.ToUpperInvariant(token[0]));
            if (token.Length > 1)
            {
                sb.Append(token.ToString(1, token.Length - 1));
            }

            token.Clear();
        }

        foreach (var c in tableName)
        {
            if (char.IsLetterOrDigit(c))
            {
                token.Append(c);
                continue;
            }

            if (c == '_')
            {
                FlushToken();
                continue;
            }

            if (c == '-')
            {
                // User-requested: replace hyphens with a non-colliding token.
                FlushToken();
                sb.Append("__");
                continue;
            }

            // General non-identifier escaping: stable, readable-ish, and extremely unlikely to collide.
            FlushToken();
            sb.Append("__u");
            sb.Append(((int)c).ToString("X4", CultureInfo.InvariantCulture));
            sb.Append("__");
        }

        FlushToken();

        if (sb.Length == 0)
        {
            sb.Append('_');
        }

        if (char.IsDigit(sb[0]))
        {
            sb.Insert(0, '_');
        }

        return sb.ToString();
    }

    /// <summary>
    /// Normalizes a DBD column name into a CLR property name.
    /// </summary>
    /// <param name="columnName">The source column name.</param>
    /// <returns>A normalized CLR property name.</returns>
    public static string NormalizePropertyName(string columnName)
    {
        // Don't normalize Field_X_Y_Z style columns
        if (columnName.StartsWith("Field_", StringComparison.InvariantCultureIgnoreCase))
        {
            return columnName;
        }

        if (columnName.EndsWith("_lang", StringComparison.Ordinal))
        {
            return ToPascalCase(columnName.Substring(0, columnName.Length - 5));
        }

        return columnName.IndexOf('_') switch
        {
            >= 0 => ToPascalCase(columnName),
            _ => columnName,
        };
    }

    /// <summary>
    /// Makes the provided name unique within the set of previously used names.
    /// </summary>
    /// <param name="name">The base name.</param>
    /// <param name="used">The set of names already used.</param>
    /// <returns>A unique name.</returns>
    public static string MakeUnique(string name, HashSet<string> used)
    {
        if (used.Add(name))
        {
            return name;
        }

        for (var i = 2; ; i++)
        {
            var candidate = name + i.ToString(CultureInfo.InvariantCulture);
            if (used.Add(candidate))
            {
                return candidate;
            }
        }
    }

    /// <summary>
    /// Escapes an identifier if it is a C# keyword.
    /// </summary>
    /// <param name="identifier">The identifier to escape.</param>
    /// <returns>The escaped identifier.</returns>
    public static string EscapeIdentifier(string identifier)
    {
        if (SyntaxFacts.GetKeywordKind(identifier) != SyntaxKind.None)
        {
            return "@" + identifier;
        }

        return identifier;
    }

    private static string ToPascalCase(string value)
    {
        var parts = value.Split(['_'], StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0)
        {
            return value;
        }

        var sb = new StringBuilder();
        foreach (var part in parts.Where(static p => p is { Length: > 0 }))
        {
            if (part.Equals("ID", StringComparison.Ordinal))
            {
                sb.Append("ID");
                continue;
            }

            sb.Append(char.ToUpperInvariant(part[0]));
            if (part.Length > 1)
            {
                sb.Append(part.Substring(1));
            }
        }

        return sb.ToString();
    }
}