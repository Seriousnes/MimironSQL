namespace MimironSQL.Providers;

/// <summary>
/// Provides a way to derive a build identity from a World of Warcraft installation.
/// </summary>
public interface IWowBuildIdentityProvider
{
    /// <summary>
    /// Gets build identity information for the installation rooted at <paramref name="installRoot"/>.
    /// </summary>
    /// <param name="installRoot">Root directory of the World of Warcraft installation.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The build identity.</returns>
    ValueTask<WowBuildIdentity> GetAsync(string installRoot, CancellationToken cancellationToken = default);
}
