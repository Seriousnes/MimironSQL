using Microsoft.Extensions.DependencyInjection;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.EntityFrameworkCore;
using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Diagnostics;
using MimironSQL.EntityFrameworkCore.Query;
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

        AddCoreServices(services);

        return services;
    }

    internal static void AddCoreServices(IServiceCollection services)
    {
        new EntityFrameworkServicesBuilder(services).TryAddCoreServices();

        // EF Core's internal diagnostics logger requires a concrete provider-specific implementation.
        services.TryAddSingleton<LoggingDefinitions, MimironDb2LoggingDefinitions>();

        // EF Core validates that a provider is configured by resolving at least one
        // Microsoft.EntityFrameworkCore.Storage.IDatabaseProvider that reports IsConfigured(options) == true.
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IDatabaseProvider, DatabaseProvider<MimironDb2OptionsExtension>>());

        // EF Core requires an IDatabase implementation for DbContext initialization.
        // This provider is read-only; SaveChanges is not supported.
        services.TryAddScoped<IDatabase, MimironDb2Database>();

        services.AddSingleton<IDb2Format>(_ => Wdc5Format.Instance);
        services.AddSingleton<IMimironDb2Store, MimironDb2Store>();

        services.AddSingleton<IMimironDb2Db2ModelProvider, MimironDb2Db2ModelProvider>();

#pragma warning disable EF1001 // Internal EF Core API usage is intentional for provider implementation.
    services.AddScoped<IQueryCompiler, MimironDb2QueryCompiler>();
#pragma warning restore EF1001
    }
}
