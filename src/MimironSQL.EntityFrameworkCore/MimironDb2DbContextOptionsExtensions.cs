using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

using MimironSQL.EntityFrameworkCore.Infrastructure;

namespace MimironSQL.EntityFrameworkCore;

public static class MimironDb2DbContextOptionsExtensions
{
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
                "MimironDb2 providers must be configured. Call UseFileSystem(...), UseCascNet(...), or another provider method inside UseMimironDb2(...). ");
        }

        return optionsBuilder;
    }

    private static MimironDb2OptionsExtension GetOrCreateExtension(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>()
            ?? new MimironDb2OptionsExtension();
}
