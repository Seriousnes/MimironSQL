namespace MimironSQL.Providers;

/// <summary>
/// Resolves DB2 names/paths to FileDataId values.
/// Implementations may use GitHub release assets (e.g., wow-listfile or WoWDBDefs) and cache as desired.
/// </summary>
public interface IManifestProvider
{
    /// <summary>
    /// Ensures the underlying manifest data exists locally (download/cache as needed).
    /// </summary>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task that completes when the manifest is available locally.</returns>
    Task EnsureManifestExistsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Attempts to resolve a DB2 table name (e.g. "SpellName") or a DB2 CASC path
    /// (e.g. "DBFilesClient\\SpellName.db2") to a FileDataId.
    /// Returns null when no mapping exists.
    /// </summary>
    /// <param name="db2NameOrPath">The DB2 table name or DB2 CASC path.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task that produces the resolved FileDataId, or null when not found.</returns>
    Task<int?> TryResolveDb2FileDataIdAsync(string db2NameOrPath, CancellationToken cancellationToken = default);
}
