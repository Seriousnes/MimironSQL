using System.Collections.Immutable;

using Microsoft.CodeAnalysis.Text;

namespace CASC.Net.Generators;

internal static class DbdParser
{
    public static ParsedTable? TryParse(string tableName, SourceText? text)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            return null;

        if (text is null)
            return null;

        var section = DbdSection.None;

        var columns = ImmutableArray.CreateBuilder<ColumnSpec>();
        var keys = ImmutableArray.CreateBuilder<KeySpec>();
        var foreignKeys = ImmutableArray.CreateBuilder<ForeignKeySpec>();
        var versions = ImmutableArray.CreateBuilder<DbdVersionDefinition>();

        var inColumnDefinitions = false;
        var sawEndOfColumns = false;

        // Current in-progress version block (separated by a blank line).
        var currentLayoutHashes = ImmutableArray.CreateBuilder<uint>();
        var currentBuilds = ImmutableArray.CreateBuilder<DbdBuildSpec>();
        var currentPhysicalColumns = ImmutableArray.CreateBuilder<DbdPhysicalColumnSpec>();

        void FinalizeCurrentVersionBlockIfAny()
        {
            if (currentBuilds.Count == 0 && currentLayoutHashes.Count == 0 && currentPhysicalColumns.Count == 0)
                return;

            // BUILD is required by the format, but some historical defs are imperfect.
            // Keep the block if it has any meaningful content.
            versions.Add(new DbdVersionDefinition(
                currentLayoutHashes.ToImmutable(),
                currentBuilds.ToImmutable(),
                currentPhysicalColumns.ToImmutable()));

            currentLayoutHashes.Clear();
            currentBuilds.Clear();
            currentPhysicalColumns.Clear();
        }

        foreach (var line in EnumerateLines(text))
        {
            var trimmed = line.Trim();

            // Blank line is meaningful: it terminates the COLUMNS section and separates version blocks.
            if (trimmed.Length == 0)
            {
                if (inColumnDefinitions)
                {
                    inColumnDefinitions = false;
                    sawEndOfColumns = true;
                }
                else if (sawEndOfColumns)
                {
                    FinalizeCurrentVersionBlockIfAny();
                }

                continue;
            }

            if (trimmed.StartsWith("//", StringComparison.Ordinal) || trimmed.StartsWith("#", StringComparison.Ordinal))
                continue;

            if (IsAllCapsSectionHeader(trimmed))
            {
                section = trimmed.ToUpperInvariant() switch
                {
                    "COLUMNS" => DbdSection.Columns,
                    "KEYS" => DbdSection.Keys,
                    _ => DbdSection.Other,
                };

                inColumnDefinitions = section == DbdSection.Columns;
                continue;
            }

            if (section == DbdSection.Columns)
            {
                // Column definitions are only valid until the first blank line.
                if (!inColumnDefinitions)
                    continue;

                var withoutComment = StripLineComment(trimmed);
                if (withoutComment.Length == 0)
                    continue;

                if (!TrySplitFirstWhitespace(withoutComment, out var dbdType, out var rest))
                    continue;

                // The column name is the next token.
                if (!TrySplitFirstWhitespace(rest, out var name, out _))
                    name = rest;

                dbdType = dbdType.Trim();
                name = name.Trim();
                if (dbdType.Length == 0 || name.Length == 0)
                    continue;

                // In WoWDBDefs, '?' indicates "unverified" not "nullable".
                name = StripTrailingQuestionMark(name);

                // Array lengths and field sizes are specified in version blocks, not in COLUMNS.
                columns.Add(new ColumnSpec(dbdType, name, ArrayLength: null));

                if (TryParseForeignKey(dbdType, out var targetTableName, out var targetColumnName))
                    foreignKeys.Add(new ForeignKeySpec(name, targetTableName, targetColumnName));

                continue;
            }

            if (section == DbdSection.Keys)
            {
                // DBD key lines are build/schema-dependent; keep parsing flexible.
                // Accept whitespace/comma separated column names, ignoring obvious prefixes.
                var tokens = trimmed.Split(new[] { ' ', '\t', ',' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(t => t.Trim())
                    .Where(t => t.Length > 0)
                    .ToArray();

                if (tokens.Length == 0)
                    continue;

                var tokenOffset = 0;
                var first = tokens[0];
                if (string.Equals(first, "PRIMARY", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(first, "PRIMARYKEY", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(first, "PRIMARY_KEY", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(first, "KEY", StringComparison.OrdinalIgnoreCase))
                {
                    tokenOffset = 1;
                }

                var cols = tokens.Skip(tokenOffset)
                    .Select(t => t.Trim())
                    .Where(t => t.Length > 0)
                    .ToImmutableArray();

                if (cols.Length > 0)
                    keys.Add(new KeySpec(cols));

                continue;
            }

            // After the COLUMNS section ends, interpret the remainder as version definitions.
            if (!sawEndOfColumns)
                continue;

            if (trimmed.StartsWith("LAYOUT ", StringComparison.Ordinal))
            {
                var payload = trimmed.Substring("LAYOUT ".Length).Trim();
                foreach (var part in payload.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries))
                {
                    var token = part.Trim();
                    if (token.Length == 0)
                        continue;

                    if (uint.TryParse(token, System.Globalization.NumberStyles.HexNumber, provider: null, out var hash))
                        currentLayoutHashes.Add(hash);
                }

                continue;
            }

            if (trimmed.StartsWith("BUILD ", StringComparison.Ordinal))
            {
                var payload = trimmed.Substring("BUILD ".Length).Trim();
                foreach (var part in payload.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries))
                {
                    var token = part.Trim();
                    if (token.Length == 0)
                        continue;

                    var dash = token.IndexOf('-');
                    if (dash > 0 && dash < token.Length - 1)
                    {
                        var from = token.Substring(0, dash).Trim();
                        var to = token.Substring(dash + 1).Trim();
                        if (from.Length > 0 && to.Length > 0)
                            currentBuilds.Add(new DbdBuildSpec.Range(from, to));
                    }
                    else
                    {
                        currentBuilds.Add(new DbdBuildSpec.Exact(token));
                    }
                }

                continue;
            }

            if (trimmed.StartsWith("COMMENT", StringComparison.Ordinal))
                continue;

            // Physical column line within a version block.
            var withoutInlineComment = StripLineComment(trimmed);
            if (withoutInlineComment.Length == 0)
                continue;

            if (TryParsePhysicalColumn(withoutInlineComment, out var physical))
                currentPhysicalColumns.Add(physical);
        }

        FinalizeCurrentVersionBlockIfAny();

        if (columns.Count == 0)
            return null;

        return new ParsedTable(tableName.Trim(), columns.ToImmutable(), keys.ToImmutable(), foreignKeys.ToImmutable(), versions.ToImmutable());
    }

    private enum DbdSection
    {
        None,
        Columns,
        Keys,
        Other,
    }

    private static IEnumerable<string> EnumerateLines(SourceText text)
    {
        foreach (var line in text.Lines)
            yield return line.ToString();
    }

    private static bool IsAllCapsSectionHeader(string line)
    {
        // A conservative heuristic: if it's all letters/underscores and has at least one letter, treat it as a section.
        var hasLetter = false;
        foreach (var c in line)
        {
            if (char.IsLetter(c))
            {
                hasLetter = true;
                if (!char.IsUpper(c))
                    return false;

                continue;
            }

            if (c == '_')
                continue;

            return false;
        }

        return hasLetter;
    }

    private static bool TrySplitFirstWhitespace(string value, out string first, out string remainder)
    {
        for (var i = 0; i < value.Length; i++)
        {
            if (value[i] is ' ' or '\t')
            {
                first = value.Substring(0, i);
                remainder = value.Substring(i).Trim();
                return first.Length > 0 && remainder.Length > 0;
            }
        }

        first = string.Empty;
        remainder = string.Empty;
        return false;
    }

    private static string StripLineComment(string value)
    {
        var index = value.IndexOf("//", StringComparison.Ordinal);
        if (index < 0)
            return value.Trim();

        return value.Substring(0, index).Trim();
    }

    private static string StripTrailingQuestionMark(string value)
        => value.EndsWith("?", StringComparison.Ordinal) ? value.Substring(0, value.Length - 1) : value;

    private static bool TryParsePhysicalColumn(string value, out DbdPhysicalColumnSpec physical)
    {
        // Format: [annotations]ColName[<Size>][[Length]]
        // Examples:
        //   $id$ID<32>
        //   $noninline,relation$SpellID<32>
        //   Attributes<32>[15]
        physical = default!;

        var s = value.Trim();
        if (s.Length == 0)
            return false;

        var isId = false;
        var isRelation = false;
        var isNonInline = false;

        if (s[0] == '$')
        {
            var end = s.IndexOf('$', startIndex: 1);
            if (end > 1)
            {
                var annotationPayload = s.Substring(1, end - 1);
                foreach (var part in annotationPayload.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries))
                {
                    var tok = part.Trim();
                    if (tok.Length == 0)
                        continue;

                    if (string.Equals(tok, "id", StringComparison.OrdinalIgnoreCase))
                        isId = true;
                    else if (string.Equals(tok, "relation", StringComparison.OrdinalIgnoreCase))
                        isRelation = true;
                    else if (string.Equals(tok, "noninline", StringComparison.OrdinalIgnoreCase))
                        isNonInline = true;
                }

                s = s.Substring(end + 1).Trim();
            }
        }

        if (s.Length == 0)
            return false;

        // Parse optional array length "[N]".
        int? arrayLength = null;
        var bracketIndex = s.IndexOf('[');
        if (bracketIndex >= 0 && s.EndsWith("]", StringComparison.Ordinal))
        {
            var lenPart = s.Substring(bracketIndex + 1, s.Length - bracketIndex - 2);
            if (int.TryParse(lenPart, out var len))
                arrayLength = len;

            s = s.Substring(0, bracketIndex).Trim();
        }

        // Strip optional size "<...>".
        var lt = s.IndexOf('<');
        if (lt >= 0)
            s = s.Substring(0, lt).Trim();

        if (s.Length == 0)
            return false;

        physical = new DbdPhysicalColumnSpec(s, arrayLength, isId, isRelation, isNonInline);
        return true;
    }

    private static bool TryParseForeignKey(string dbdType, out string targetTableName, out string targetColumnName)
    {
        // WoWDBDefs foreign key syntax: type<ForeignDB::ForeignCol>
        // Only treat it as a relation if the generic payload contains "::".
        targetTableName = string.Empty;
        targetColumnName = string.Empty;

        if (string.IsNullOrWhiteSpace(dbdType))
            return false;

        var lt = dbdType.IndexOf('<');
        if (lt < 0)
            return false;

        var gt = dbdType.LastIndexOf('>');
        if (gt <= lt)
            return false;

        var inner = dbdType.Substring(lt + 1, gt - lt - 1).Trim();
        var sep = inner.IndexOf("::", StringComparison.Ordinal);
        if (sep < 0)
            return false;

        targetTableName = inner.Substring(0, sep).Trim();
        targetColumnName = inner.Substring(sep + 2).Trim();

        return targetTableName.Length > 0 && targetColumnName.Length > 0;
    }
}
