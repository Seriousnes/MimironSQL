namespace MimironSQL.Dbd;

/// <summary>
/// Represents a BUILD block within a DBD layout.
/// </summary>
public sealed class DbdBuildBlock : IDbdBuildBlock
{
    private string _buildLine;

    /// <summary>
    /// Initializes a new instance of the <see cref="DbdBuildBlock"/> class.
    /// </summary>
    public DbdBuildBlock(string buildLine)
    {
        _buildLine = buildLine;
    }

    /// <summary>
    /// Gets the raw BUILD line text.
    /// </summary>
    public string BuildLine => _buildLine;

    internal void AppendBuildLine(string buildLine)
    {
        if (string.IsNullOrWhiteSpace(buildLine))
        {
            return;
        }

        var text = buildLine.Trim();
        if (text.StartsWith("BUILD ", StringComparison.Ordinal))
        {
            text = text.Substring("BUILD ".Length).Trim();
        }

        if (text.Length == 0)
        {
            return;
        }

        _buildLine = $"{BuildLine}, {text}";
    }

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
            {
                continue;
            }

            count++;
        }

        return count;
    }
}
