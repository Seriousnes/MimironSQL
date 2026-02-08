using Microsoft.EntityFrameworkCore.Infrastructure;
using MimironSQL.EntityFrameworkCore;
using MimironSQL.Providers;

namespace Microsoft.EntityFrameworkCore;

public static class MimironDb2DbContextOptionsExtensions
{
    public static DbContextOptionsBuilder UseMimironDb2(
        this DbContextOptionsBuilder optionsBuilder,
        IDb2StreamProvider db2Provider,
        IDbdProvider dbdProvider,
        ITactKeyProvider tactKeyProvider,
        Action<MimironDb2DbContextOptionsBuilder>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentNullException.ThrowIfNull(db2Provider);
        ArgumentNullException.ThrowIfNull(dbdProvider);
        ArgumentNullException.ThrowIfNull(tactKeyProvider);

        var extension = GetOrCreateExtension(optionsBuilder);
        extension = extension.WithProviders(db2Provider, dbdProvider, tactKeyProvider);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        // Provider default: tracking enabled for EF-like behaviors (e.g., lazy loading).
        optionsBuilder.UseQueryTrackingBehavior(QueryTrackingBehavior.TrackAll);

        configureOptions?.Invoke(new MimironDb2DbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

    private static MimironDb2OptionsExtension GetOrCreateExtension(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>()
            ?? new MimironDb2OptionsExtension();
}
