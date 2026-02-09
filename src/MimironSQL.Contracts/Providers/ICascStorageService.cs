namespace MimironSQL.Providers;

public interface ICascStorageService
{
    Task<ICascStorage> OpenInstallRootAsync(string installRoot, CancellationToken cancellationToken = default);
}
