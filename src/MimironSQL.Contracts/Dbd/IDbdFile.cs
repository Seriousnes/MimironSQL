namespace MimironSQL.Dbd;

/// <summary>
/// Represents a parsed DBD definition for a DB2 table.
/// </summary>
public interface IDbdFile
{
    /// <summary>
    /// Gets columns keyed by their name.
    /// </summary>
    IReadOnlyDictionary<string, IDbdColumn> ColumnsByName { get; }

    /// <summary>
    /// Gets all known layouts for the table.
    /// </summary>
    IReadOnlyList<IDbdLayout> Layouts { get; }

    /// <summary>
    /// Gets build blocks that apply globally.
    /// </summary>
    IReadOnlyList<IDbdBuildBlock> GlobalBuilds { get; }

    /// <summary>
    /// Attempts to find a layout by its hash.
    /// </summary>
    /// <param name="layoutHash">The layout hash.</param>
    /// <param name="layout">When successful, receives the layout.</param>
    /// <returns><see langword="true"/> if found; otherwise <see langword="false"/>.</returns>
    bool TryGetLayout(uint layoutHash, out IDbdLayout layout);
}
