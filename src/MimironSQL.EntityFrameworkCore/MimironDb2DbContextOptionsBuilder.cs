using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

using MimironSQL.EntityFrameworkCore.Infrastructure;

namespace MimironSQL.EntityFrameworkCore;

/// <summary>
/// Fluent builder used by <see cref="MimironDb2DbContextOptionsExtensions.UseMimironDb2"/> to select and configure a DB2 provider.
/// </summary>
public sealed class MimironDb2DbContextOptionsBuilder : IMimironDb2DbContextOptionsBuilder
{
    internal MimironDb2DbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder, MimironDb2OptionsExtension extension)
    {
        OptionsBuilder = optionsBuilder;
        Extension = extension;
    }

    /// <summary>
    /// The EF Core options builder.
    /// </summary>
    public DbContextOptionsBuilder OptionsBuilder { get; }

    internal MimironDb2OptionsExtension Extension { get; private set; }

    /// <inheritdoc />
    public IMimironDb2DbContextOptionsBuilder WithWowVersion(string wowVersion)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(wowVersion);
        Extension = Extension.WithWowVersion(wowVersion);
        return this;
    }

    /// <inheritdoc />
    public IMimironDb2DbContextOptionsBuilder ConfigureProvider(
        string providerKey,
        int providerConfigHash,
        Action<IServiceCollection> applyProviderServices)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerKey);
        ArgumentNullException.ThrowIfNull(applyProviderServices);

        Extension = Extension.WithProvider(providerKey, providerConfigHash, applyProviderServices);
        return this;
    }

    internal void SetForeignKeyArrayModeling(ForeignKeyArrayModeling modeling)
        => Extension = Extension.WithForeignKeyArrayModeling(modeling);

    internal void SetRelaxLayoutValidation(bool relaxLayoutValidation)
        => Extension = Extension.WithRelaxLayoutValidation(relaxLayoutValidation);

    internal void SetEagerSparseOffsetTable(bool eagerSparseOffsetTable)
        => Extension = Extension.WithEagerSparseOffsetTable(eagerSparseOffsetTable);
}
