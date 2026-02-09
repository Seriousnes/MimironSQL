namespace MimironSQL.Providers;

public interface ICascStorage
{
    Task<Stream> OpenDb2ByFileDataIdAsync(int fileDataId, CancellationToken cancellationToken = default);
}
