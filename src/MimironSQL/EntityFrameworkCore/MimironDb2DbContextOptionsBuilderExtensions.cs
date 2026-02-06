using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using MimironSQL.EntityFrameworkCore.Infrastructure;

namespace Microsoft.EntityFrameworkCore;

public static class MimironDb2DbContextOptionsBuilderExtensions
{
    public static DbContextOptionsBuilder<TContext> UseMimironDb2<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        Action<MimironDb2OptionsBuilder>? configure = null)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);

        UseMimironDb2((DbContextOptionsBuilder)optionsBuilder, configure);
        return optionsBuilder;
    }

    public static DbContextOptionsBuilder UseMimironDb2(
        this DbContextOptionsBuilder optionsBuilder,
        Action<MimironDb2OptionsBuilder>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(new MimironDb2OptionsExtension());

        optionsBuilder.UseQueryTrackingBehavior(QueryTrackingBehavior.NoTracking);

        var builder = new MimironDb2OptionsBuilder(optionsBuilder);
        configure?.Invoke(builder);

        optionsBuilder.AddInterceptors(ReadOnlySaveChangesInterceptor.Instance);

        return optionsBuilder;
    }
}
