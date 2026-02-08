using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using MimironSQL.EntityFrameworkCore.ChangeTracking;
using MimironSQL.EntityFrameworkCore.Diagnostics;
using MimironSQL.EntityFrameworkCore.Infrastructure;
using MimironSQL.EntityFrameworkCore.Query;
using MimironSQL.EntityFrameworkCore.Query.Internal;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;

namespace MimironSQL.EntityFrameworkCore;

public static class MimironDb2ServiceCollectionExtensions
{
    internal static void AddCoreServices(IServiceCollection services)
    {
        new EntityFrameworkServicesBuilder(services).TryAddCoreServices();

        services.Replace(ServiceDescriptor.Scoped<ILazyLoader, MimironDb2LazyLoader>());

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IInterceptor, MimironDb2ReadOnlySaveChangesInterceptor>());

        // EF Core's internal diagnostics logger requires a concrete provider-specific implementation.
        services.TryAddSingleton<LoggingDefinitions, MimironDb2LoggingDefinitions>();

        // EF Core validates that a provider is configured by resolving at least one
        // Microsoft.EntityFrameworkCore.Storage.IDatabaseProvider that reports IsConfigured(options) == true.
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IDatabaseProvider, DatabaseProvider<MimironDb2OptionsExtension>>());

        // EF Core requires an IDatabase implementation for DbContext initialization.
        // This provider is read-only; SaveChanges is not supported.
        services.TryAddScoped<IDatabase, MimironDb2Database>();

        services.TryAddSingleton<ITypeMappingSource, MimironDb2TypeMappingSource>();

    services.AddSingleton<IDb2Format, Wdc5Format>();
        services.AddSingleton<IMimironDb2Store, MimironDb2Store>();

        services.TryAddScoped<IMimironDb2Db2ModelProvider, MimironDb2Db2ModelProvider>();

        services.TryAddScoped<IMimironDb2QueryExecutor, MimironDb2QueryExecutor>();
        MimironDb2EfCoreInternalServiceRegistration.Add(services);
    }
}
