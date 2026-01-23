namespace MimironSQL.Db2.Schema.Dbd;

internal sealed class DbdFile(Dictionary<string, DbdColumn> columnsByName, List<DbdLayout> layouts)
{
    public IReadOnlyDictionary<string, DbdColumn> ColumnsByName { get; } = columnsByName;
    public IReadOnlyList<DbdLayout> Layouts { get; } = layouts;

    public static DbdFile Parse(Stream stream)
    {
        using var reader = new StreamReader(stream);

        var columnsByName = new Dictionary<string, DbdColumn>(StringComparer.Ordinal);
        var layouts = new List<DbdLayout>();

        DbdLayout? currentLayout = null;
        var activeBuilds = new List<DbdBuildBlock>();
        var entriesStartedForActiveBuilds = false;

        var inColumns = false;
        while (!reader.EndOfStream)
        {
            var rawLine = reader.ReadLine();
            if (rawLine is null)
                continue;

            var line = StripComment(rawLine).Trim();
            if (line.Length == 0)
                continue;

            if (line.Equals("COLUMNS", StringComparison.Ordinal))
            {
                inColumns = true;
                continue;
            }

            if (line.StartsWith("COMMENT", StringComparison.Ordinal))
                continue;

            if (line.StartsWith("LAYOUT ", StringComparison.Ordinal))
            {
                inColumns = false;
                activeBuilds.Clear();
                entriesStartedForActiveBuilds = false;

                currentLayout = DbdLayout.ParseHeader(line);
                layouts.Add(currentLayout);
                continue;
            }

            if (line.StartsWith("BUILD ", StringComparison.Ordinal))
            {
                inColumns = false;
                if (currentLayout is null)
                    continue;

                var newBuild = new DbdBuildBlock(line);
                currentLayout.Builds.Add(newBuild);

                // DBDs often list multiple BUILD lines, then a single set of entries that applies to all of them.
                // Once we start reading entries, a new BUILD indicates a new entry block.
                if (entriesStartedForActiveBuilds)
                {
                    activeBuilds.Clear();
                    entriesStartedForActiveBuilds = false;
                }

                activeBuilds.Add(newBuild);
                continue;
            }

            if (inColumns)
            {
                if (DbdColumnParser.TryParse(line, out var name, out var column))
                    columnsByName[name] = column;
                continue;
            }

            if (activeBuilds.Count != 0)
            {
                if (DbdLayoutEntryParser.TryParse(line, columnsByName, out var entry))
                {
                    entriesStartedForActiveBuilds = true;
                    foreach (var b in activeBuilds)
                        b.Entries.Add(entry);
                }
            }
        }

        return new DbdFile(columnsByName, layouts);
    }

    public bool TryGetLayout(uint layoutHash, out DbdLayout layout)
    {
        foreach (var candidate in Layouts)
        {
            if (candidate.ContainsHash(layoutHash))
            {
                layout = candidate;
                return true;
            }
        }

        layout = default!;
        return false;
    }

    private static string StripComment(string line)
    {
        var idx = line.IndexOf("//", StringComparison.Ordinal);
        return idx < 0 ? line : line[..idx];
    }
}
