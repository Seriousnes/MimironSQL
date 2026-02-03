namespace MimironSQL.Providers;

public interface IWowListfileProvider : IGitHubReleaseAssetProvider
{
    /// <summary>
    /// Streams listfile records from the cached CSV using CsvHelper.
    /// </summary>
    IAsyncEnumerable<ListfileRecord> ReadRecordsAsync(CancellationToken cancellationToken = default);
}
