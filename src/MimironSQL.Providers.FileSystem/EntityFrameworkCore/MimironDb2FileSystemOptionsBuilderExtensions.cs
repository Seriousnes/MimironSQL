using Microsoft.Extensions.DependencyInjection;

using MimironSQL.EntityFrameworkCore;

namespace MimironSQL.Providers;

public static class MimironDb2FileSystemOptionsBuilderExtensions
{
    public static MimironDb2DbContextOptionsBuilder UseFileSystem(
        this MimironDb2DbContextOptionsBuilder builder,
        string db2DirectoryPath,
        string dbdDefinitionsDirectory)
        => builder.UseFileSystem(
            new FileSystemDb2StreamProviderOptions(db2DirectoryPath),
            new FileSystemDbdProviderOptions(dbdDefinitionsDirectory));

    public static MimironDb2DbContextOptionsBuilder UseFileSystem(
        this MimironDb2DbContextOptionsBuilder builder,
        FileSystemDb2StreamProviderOptions db2Options,
        FileSystemDbdProviderOptions dbdOptions)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(db2Options);
        ArgumentNullException.ThrowIfNull(dbdOptions);

        var configHash = HashCode.Combine(db2Options.Db2DirectoryPath, dbdOptions.DefinitionsDirectory);

        return builder.ConfigureProvider(
            providerKey: "FileSystem",
            providerConfigHash: configHash,
            applyProviderServices: services =>
            {
                services.AddSingleton(db2Options);
                services.AddSingleton(dbdOptions);

                services.AddSingleton<IDb2StreamProvider, FileSystemDb2StreamProvider>();
                services.AddSingleton<IDbdProvider, FileSystemDbdProvider>();
            });
    }
}
