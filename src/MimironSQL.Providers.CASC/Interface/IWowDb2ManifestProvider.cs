namespace MimironSQL.Providers;

public interface IWowDb2ManifestProvider : IGitHubReleaseAssetProvider
{
    /// <summary>
    /// Returns a mapping from canonical DB2 path (e.g. "DBFilesClient\\SpellName.db2") to FileDataId.
    /// </summary>
    Task<IReadOnlyDictionary<string, int>> GetDb2FileDataIdByPathAsync(CancellationToken cancellationToken = default);
}
