namespace MimironSQL.Dbd;

public sealed class DbdFile(Dictionary<string, DbdColumn> columnsByName, List<DbdLayout> layouts, List<DbdBuildBlock> globalBuilds) : IDbdFile
{
    private readonly IReadOnlyDictionary<string, IDbdColumn> _columnsByNameContract = columnsByName.ToDictionary(static x => x.Key, static x => (IDbdColumn)x.Value);

    public IReadOnlyDictionary<string, DbdColumn> ColumnsByName { get; } = columnsByName;
    public IReadOnlyList<DbdLayout> Layouts { get; } = layouts;
    public IReadOnlyList<DbdBuildBlock> GlobalBuilds { get; } = globalBuilds;

    IReadOnlyDictionary<string, IDbdColumn> IDbdFile.ColumnsByName => _columnsByNameContract;
    IReadOnlyList<IDbdLayout> IDbdFile.Layouts => Layouts;
    IReadOnlyList<IDbdBuildBlock> IDbdFile.GlobalBuilds => GlobalBuilds;

    public static DbdFile Parse(Stream stream)
    {
        using var reader = new StreamReader(stream);

        var columnsByName = new Dictionary<string, DbdColumn>(StringComparer.Ordinal);
        var layouts = new List<DbdLayout>();
        var globalBuilds = new List<DbdBuildBlock>();

        DbdLayout? currentLayout = null;
        var activeBuilds = new List<DbdBuildBlock>();
        var entriesStartedForActiveBuilds = false;

        var inColumns = false;
        while (!reader.EndOfStream)
        {
            var rawLine = reader.ReadLine();
            if (rawLine is null)
                continue;

            var line = StripComment(rawLine.AsSpan()).Trim();
            if (line is { Length: 0 })
                continue;

            if (line.Equals("COLUMNS".AsSpan(), StringComparison.Ordinal))
            {
                inColumns = true;
                continue;
            }

            if (line.StartsWith("COMMENT".AsSpan(), StringComparison.Ordinal))
                continue;

            if (line.StartsWith("LAYOUT ".AsSpan(), StringComparison.Ordinal))
            {
                inColumns = false;
                activeBuilds.Clear();
                entriesStartedForActiveBuilds = false;

                currentLayout = DbdLayout.ParseHeader(line.ToString());
                layouts.Add(currentLayout);
                continue;
            }

            if (line.StartsWith("BUILD ".AsSpan(), StringComparison.Ordinal))
            {
                inColumns = false;
                var newBuild = new DbdBuildBlock(line.ToString());
                switch (currentLayout)
                {
                    case null:
                        globalBuilds.Add(newBuild);
                        break;
                    default:
                        currentLayout.Builds.Add(newBuild);
                        break;
                }

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
                if (DbdColumnParser.TryParse(line.ToString(), out var name, out var column))
                    columnsByName[name] = column;
                continue;
            }

            if (activeBuilds is { Count: not 0 })
            {
                if (DbdLayoutEntryParser.TryParse(line.ToString(), columnsByName, out var entry))
                {
                    entriesStartedForActiveBuilds = true;
                    foreach (var b in activeBuilds)
                        b.Entries.Add(entry);
                }
            }
        }

        return new DbdFile(columnsByName, layouts, globalBuilds);
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

    bool IDbdFile.TryGetLayout(uint layoutHash, out IDbdLayout layout)
    {
        if (TryGetLayout(layoutHash, out var concreteLayout))
        {
            layout = concreteLayout;
            return true;
        }

        layout = default!;
        return false;
    }

    private static ReadOnlySpan<char> StripComment(ReadOnlySpan<char> line)
    {
        var idx = line.IndexOf("//".AsSpan(), StringComparison.Ordinal);
        return idx < 0 ? line : line.Slice(0, idx);
    }
}
