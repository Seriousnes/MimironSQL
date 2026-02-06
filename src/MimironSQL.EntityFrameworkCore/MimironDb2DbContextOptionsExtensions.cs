using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace MimironSQL.EntityFrameworkCore;

public static class MimironDb2DbContextOptionsExtensions
{
    public static DbContextOptionsBuilder UseMimironDb2FileSystem(
        this DbContextOptionsBuilder optionsBuilder,
        string db2DirectoryPath,
        string? dbdDefinitionsPath = null,
        Action<MimironDb2DbContextOptionsBuilder>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(db2DirectoryPath);

        var extension = GetOrCreateExtension(optionsBuilder);
        extension = extension.WithFileSystem(db2DirectoryPath, dbdDefinitionsPath);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        configureOptions?.Invoke(new MimironDb2DbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

    public static DbContextOptionsBuilder UseMimironDb2Casc(
        this DbContextOptionsBuilder optionsBuilder,
        string cascRootPath,
        string? dbdDefinitionsPath = null,
        Action<MimironDb2DbContextOptionsBuilder>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(cascRootPath);

        var extension = GetOrCreateExtension(optionsBuilder);
        extension = extension.WithCasc(cascRootPath, dbdDefinitionsPath);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        configureOptions?.Invoke(new MimironDb2DbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

    private static MimironDb2OptionsExtension GetOrCreateExtension(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>()
            ?? new MimironDb2OptionsExtension();
}
