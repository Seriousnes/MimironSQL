namespace MimironSQL.Providers;

/// <summary>
/// Represents an opened CASC storage instance.
/// </summary>
public interface ICascStorage
{
    /// <summary>
    /// Opens a DB2 stream by file data ID.
    /// </summary>
    /// <param name="fileDataId">The file data ID of the DB2 file.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A readable stream containing DB2 data.</returns>
    Task<Stream> OpenDb2ByFileDataIdAsync(int fileDataId, CancellationToken cancellationToken = default);
}
