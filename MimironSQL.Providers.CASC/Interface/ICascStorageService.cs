namespace MimironSQL.Providers;

public interface ICascStorageService
{
    Task<CascStorage> OpenInstallRootAsync(string installRoot, CancellationToken cancellationToken = default);
}
