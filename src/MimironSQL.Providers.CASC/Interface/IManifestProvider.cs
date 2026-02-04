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
    Task EnsureManifestExistsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Attempts to resolve a DB2 table name (e.g. "SpellName") or a DB2 CASC path
    /// (e.g. "DBFilesClient\\SpellName.db2") to a FileDataId.
    /// Returns null when no mapping exists.
    /// </summary>
    Task<int?> TryResolveDb2FileDataIdAsync(string db2NameOrPath, CancellationToken cancellationToken = default);
}
