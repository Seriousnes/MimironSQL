namespace MimironSQL.Dbd;

/// <summary>
/// Represents a BUILD block within a DBD layout.
/// </summary>
public sealed class DbdBuildBlock : IDbdBuildBlock
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DbdBuildBlock"/> class.
    /// </summary>
    public DbdBuildBlock(string buildLine)
    {
        BuildLine = buildLine;
    }

    /// <summary>
    /// Gets the raw BUILD line text.
    /// </summary>
    public string BuildLine { get; }

    /// <summary>
    /// Gets the concrete layout entries for this build block.
    /// </summary>
    public List<DbdLayoutEntry> Entries { get; } = [];

    IReadOnlyList<IDbdLayoutEntry> IDbdBuildBlock.Entries => Entries;

    /// <summary>
    /// Gets the count of physical (non-noninline) columns for this build.
    /// </summary>
    public int GetPhysicalColumnCount()
    {
        var count = 0;
        foreach (var entry in Entries)
        {
            if (entry.IsNonInline)
                continue;

            count++;
        }

        return count;
    }
}
