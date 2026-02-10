using Microsoft.Extensions.DependencyInjection;

using MimironSQL.EntityFrameworkCore;

namespace MimironSQL.Providers;

/// <summary>
/// Provider configuration extensions for using extracted DB2 files and DBD definitions from the local file system.
/// </summary>
public static class MimironDb2FileSystemOptionsBuilderExtensions
{
    /// <summary>
    /// Configures the DB2 provider to read DB2 files and DBD definitions from the file system.
    /// </summary>
    /// <param name="builder">The provider options builder.</param>
    /// <param name="db2DirectoryPath">Directory containing DB2 files.</param>
    /// <param name="dbdDefinitionsDirectory">Directory containing WoWDBDefs .dbd files.</param>
    /// <returns>The same <paramref name="builder"/> instance to enable chaining.</returns>
    public static IMimironDb2DbContextOptionsBuilder UseFileSystem(
        this IMimironDb2DbContextOptionsBuilder builder,
        string db2DirectoryPath,
        string dbdDefinitionsDirectory)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(db2DirectoryPath);
        ArgumentException.ThrowIfNullOrWhiteSpace(dbdDefinitionsDirectory);

        return builder.UseFileSystem(
            new FileSystemDb2StreamProviderOptions(db2DirectoryPath),
            new FileSystemDbdProviderOptions(dbdDefinitionsDirectory));
    }

    /// <summary>
    /// Configures the DB2 provider to read DB2 files and DBD definitions from the file system.
    /// </summary>
    /// <param name="builder">The provider options builder.</param>
    /// <param name="db2Options">Options for locating DB2 files.</param>
    /// <param name="dbdOptions">Options for locating DBD definitions.</param>
    /// <returns>The same <paramref name="builder"/> instance to enable chaining.</returns>
    public static IMimironDb2DbContextOptionsBuilder UseFileSystem(
        this IMimironDb2DbContextOptionsBuilder builder,
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
