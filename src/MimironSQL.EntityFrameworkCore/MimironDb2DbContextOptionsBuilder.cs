using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

using MimironSQL.EntityFrameworkCore.Infrastructure;

namespace MimironSQL.EntityFrameworkCore;

public class MimironDb2DbContextOptionsBuilder : IMimironDb2DbContextOptionsBuilder
{
    public MimironDb2DbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        OptionsBuilder = optionsBuilder;
    }

    public DbContextOptionsBuilder OptionsBuilder { get; }

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
