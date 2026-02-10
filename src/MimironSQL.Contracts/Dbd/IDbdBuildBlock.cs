namespace MimironSQL.Dbd;

/// <summary>
/// Represents a build-specific block of layout entries.
/// </summary>
public interface IDbdBuildBlock
{
    /// <summary>
    /// Gets the build selector line.
    /// </summary>
    string BuildLine { get; }

    /// <summary>
    /// Gets the layout entries for this build block.
    /// </summary>
    IReadOnlyList<IDbdLayoutEntry> Entries { get; }

    /// <summary>
    /// Calculates the physical column count for this build block.
    /// </summary>
    /// <returns>The physical column count.</returns>
    int GetPhysicalColumnCount();
}
