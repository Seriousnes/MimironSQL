namespace MimironSQL.Providers;

/// <summary>
/// Opens CASC storage instances.
/// </summary>
public interface ICascStorageService
{
    /// <summary>
    /// Opens a World of Warcraft installation root as a CASC storage.
    /// </summary>
    /// <param name="installRoot">The root directory of the WoW installation.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>An opened <see cref="ICascStorage"/> instance.</returns>
    Task<ICascStorage> OpenInstallRootAsync(string installRoot, CancellationToken cancellationToken = default);
}
