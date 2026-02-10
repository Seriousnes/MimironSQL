using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

using MimironSQL.EntityFrameworkCore.Infrastructure;

namespace MimironSQL.EntityFrameworkCore;

/// <summary>
/// Extension methods for configuring MimironSQL's DB2 Entity Framework Core provider.
/// </summary>
public static class MimironDb2DbContextOptionsExtensions
{
    /// <summary>
    /// Enables the MimironSQL DB2 provider and configures the underlying DB2 stream / DBD definition provider.
    /// </summary>
    /// <param name="optionsBuilder">The EF Core options builder.</param>
    /// <param name="configureOptions">A callback used to configure the provider.</param>
    /// <returns>The same <paramref name="optionsBuilder"/> instance to enable chaining.</returns>
    public static DbContextOptionsBuilder UseMimironDb2(
        this DbContextOptionsBuilder optionsBuilder,
        Action<IMimironDb2DbContextOptionsBuilder> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentNullException.ThrowIfNull(configureOptions);

        var extension = GetOrCreateExtension(optionsBuilder);
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        // Provider default: tracking enabled for EF-like behaviors (e.g., lazy loading).
        optionsBuilder.UseQueryTrackingBehavior(QueryTrackingBehavior.TrackAll);

        configureOptions(new MimironDb2DbContextOptionsBuilder(optionsBuilder));

        extension = GetOrCreateExtension(optionsBuilder);
        if (extension.ProviderKey is null)
        {
            throw new InvalidOperationException(
                "MimironDb2 providers must be configured. Call UseFileSystem(...), UseCasc(...), or another provider method inside UseMimironDb2(...). ");
        }

        return optionsBuilder;
    }

    private static MimironDb2OptionsExtension GetOrCreateExtension(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>()
            ?? new MimironDb2OptionsExtension();
}
