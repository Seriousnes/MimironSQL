using Microsoft.Extensions.DependencyInjection;
using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;
using MimironSQL.Providers;

namespace MimironSQL.EntityFrameworkCore;

public static class MimironDb2ServiceCollectionExtensions
{
    public static IServiceCollection AddMimironDb2FileSystem(
        this IServiceCollection services,
        string db2DirectoryPath,
        string? dbdDefinitionsPath = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(db2DirectoryPath);

        var dbdPath = dbdDefinitionsPath ?? Path.Combine(db2DirectoryPath, "definitions");

        services.AddSingleton<IDb2StreamProvider>(_ =>
            new FileSystemDb2StreamProvider(new FileSystemDb2StreamProviderOptions(db2DirectoryPath)));

        services.AddSingleton<IDbdProvider>(_ =>
            new FileSystemDbdProvider(new FileSystemDbdProviderOptions(dbdPath)));

        services.AddSingleton<IDb2Format>(_ => Wdc5Format.Instance);
        services.AddSingleton<IMimironDb2Store, MimironDb2Store>();

        return services;
    }
}
