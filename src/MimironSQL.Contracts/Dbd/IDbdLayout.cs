namespace MimironSQL.Dbd;

/// <summary>
/// Represents a DBD layout group (one or more hashes sharing build blocks).
/// </summary>
public interface IDbdLayout
{
    /// <summary>
    /// Gets all hashes that identify this layout.
    /// </summary>
    IReadOnlyList<uint> Hashes { get; }

    /// <summary>
    /// Gets build-specific definitions for this layout.
    /// </summary>
    IReadOnlyList<IDbdBuildBlock> Builds { get; }

    /// <summary>
    /// Returns <see langword="true"/> if this layout includes the specified hash.
    /// </summary>
    /// <param name="hash">The layout hash.</param>
    /// <returns><see langword="true"/> if contained; otherwise <see langword="false"/>.</returns>
    bool ContainsHash(uint hash);

    /// <summary>
    /// Attempts to select a build block matching a physical column count.
    /// </summary>
    /// <param name="expected">The expected physical column count.</param>
    /// <param name="build">When successful, receives the selected build block.</param>
    /// <param name="availableCounts">When unsuccessful, receives available counts for diagnostics.</param>
    /// <returns><see langword="true"/> if a build was selected; otherwise <see langword="false"/>.</returns>
    bool TrySelectBuildByPhysicalColumnCount(int expected, out IDbdBuildBlock build, out int[] availableCounts);
}
