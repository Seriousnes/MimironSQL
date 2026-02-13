using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

using MimironSQL.EntityFrameworkCore.ChangeTracking;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.EntityFrameworkCore.Diagnostics;
using MimironSQL.EntityFrameworkCore.Infrastructure;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;

namespace MimironSQL.EntityFrameworkCore;

/// <summary>
/// Registers core services for the MimironDB2 EF Core provider.
/// </summary>
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

        // ── Standard EF Core query pipeline ──
        // These use Replace to override EF Core's defaults registered by TryAddCoreServices
        services.TryAddScoped<IDatabase, MimironDb2Database>();
        services.TryAddScoped<IQueryContextFactory, Db2QueryContextFactory>();
        services.Replace(ServiceDescriptor.Scoped<IQueryableMethodTranslatingExpressionVisitorFactory, Db2QueryableMethodTranslatingExpressionVisitorFactory>());
        services.Replace(ServiceDescriptor.Scoped<IShapedQueryCompilingExpressionVisitorFactory, Db2ShapedQueryCompilingExpressionVisitorFactory>());
        services.Replace(ServiceDescriptor.Scoped<IQueryTranslationPreprocessorFactory, Db2QueryTranslationPreprocessorFactory>());

        services.TryAddSingleton<ITypeMappingSource, MimironDb2TypeMappingSource>();

        services.AddSingleton<IDb2Format, Wdc5Format>();
        services.AddScoped<IMimironDb2Store, MimironDb2Store>();

        services.TryAddScoped<IDb2ModelBinding, Db2ModelBindingProvider>();
    }
}
