using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

using MimironSQL.EntityFrameworkCore.Infrastructure;

namespace MimironSQL.EntityFrameworkCore;

/// <summary>
/// Fluent builder used by <see cref="MimironDb2DbContextOptionsExtensions.UseMimironDb2"/> to select and configure a DB2 provider.
/// </summary>
public class MimironDb2DbContextOptionsBuilder : IMimironDb2DbContextOptionsBuilder
{
    /// <summary>
    /// Creates a new builder bound to an EF Core <see cref="DbContextOptionsBuilder"/>.
    /// </summary>
    /// <param name="optionsBuilder">The EF Core options builder to configure.</param>
    public MimironDb2DbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        OptionsBuilder = optionsBuilder;
    }

    /// <summary>
    /// Gets the underlying EF Core options builder.
    /// </summary>
    public DbContextOptionsBuilder OptionsBuilder { get; }

    /// <summary>
    /// Configures the provider implementation and registers any provider-specific services.
    /// </summary>
    /// <param name="providerKey">A stable identifier for the provider (for example, <c>FileSystem</c> or <c>CASC</c>).</param>
    /// <param name="providerConfigHash">A hash representing the provider configuration to support EF Core service provider caching.</param>
    /// <param name="applyProviderServices">A callback to register provider-specific services.</param>
    /// <returns>The same builder instance to enable chaining.</returns>
    public virtual IMimironDb2DbContextOptionsBuilder ConfigureProvider(
        string providerKey,
        int providerConfigHash,
        Action<IServiceCollection> applyProviderServices)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerKey);
        ArgumentNullException.ThrowIfNull(applyProviderServices);

        var extension = GetOrCreateExtension(OptionsBuilder);
        if (extension.ProviderKey is not null && !string.Equals(extension.ProviderKey, providerKey, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"MimironDb2 provider already configured as '{extension.ProviderKey}'. It cannot be changed to '{providerKey}'.");
        }

        extension = extension.WithProvider(providerKey, providerConfigHash, applyProviderServices);
        ((IDbContextOptionsBuilderInfrastructure)OptionsBuilder).AddOrUpdateExtension(extension);

        return this;
    }

    private static MimironDb2OptionsExtension GetOrCreateExtension(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>()
           ?? new MimironDb2OptionsExtension();
}
