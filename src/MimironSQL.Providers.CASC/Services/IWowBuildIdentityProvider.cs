namespace MimironSQL.Providers;

public interface IWowBuildIdentityProvider
{
    ValueTask<WowBuildIdentity> GetAsync(string installRoot, CancellationToken cancellationToken = default);
}
