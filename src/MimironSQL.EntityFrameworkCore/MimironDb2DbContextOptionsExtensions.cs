using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

using MimironSQL.EntityFrameworkCore.Infrastructure;

namespace MimironSQL.EntityFrameworkCore;

/// <summary>
/// Provider configuration entry point for the MimironSQL DB2 EF Core provider.
/// </summary>
public static class MimironDb2DbContextOptionsExtensions
{
    /// <summary>
    /// Configures MimironSQL DB2 as the EF Core database provider.
    /// </summary>
    public static DbContextOptionsBuilder UseMimironDb2(
        this DbContextOptionsBuilder optionsBuilder,
        Action<IMimironDb2DbContextOptionsBuilder> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentNullException.ThrowIfNull(configureOptions);

        var existing = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>() ?? new MimironDb2OptionsExtension();
        var builder = new MimironDb2DbContextOptionsBuilder(optionsBuilder, existing);

        configureOptions(builder);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(builder.Extension);
        return optionsBuilder;
    }
}
