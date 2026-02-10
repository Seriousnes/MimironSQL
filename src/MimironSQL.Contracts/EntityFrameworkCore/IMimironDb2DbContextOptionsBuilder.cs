using Microsoft.Extensions.DependencyInjection;

namespace MimironSQL.EntityFrameworkCore;

/// <summary>
/// Abstraction for configuring the MimironSQL DB2 provider when building an EF Core <see cref="Microsoft.EntityFrameworkCore.DbContext"/>.
/// </summary>
public interface IMimironDb2DbContextOptionsBuilder
{
    /// <summary>
    /// Configures the provider implementation and registers any provider-specific services.
    /// </summary>
    /// <param name="providerKey">A stable identifier for the provider.</param>
    /// <param name="providerConfigHash">A hash representing the provider configuration.</param>
    /// <param name="applyProviderServices">A callback to register provider-specific services.</param>
    /// <returns>The same builder instance to enable chaining.</returns>
    IMimironDb2DbContextOptionsBuilder ConfigureProvider(
        string providerKey,
        int providerConfigHash,
        Action<IServiceCollection> applyProviderServices);
}
