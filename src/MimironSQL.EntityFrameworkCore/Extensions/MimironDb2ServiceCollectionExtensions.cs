using System.ComponentModel;

using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

using MimironSQL.EntityFrameworkCore.Diagnostics.Internal;
using MimironSQL.EntityFrameworkCore.Infrastructure;
using MimironSQL.EntityFrameworkCore.Query.Internal;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.EntityFrameworkCore.Storage.Internal;
using MimironSQL.Dbd;
using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;
using MimironSQL.EntityFrameworkCore.Model;
using MimironSQL.Providers;

// ReSharper disable once CheckNamespace
namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// MimironSQL DB2-specific extension methods for <see cref="IServiceCollection"/>.
/// </summary>
public static class MimironDb2ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the services required by the MimironSQL DB2 database provider for Entity Framework.
    /// </summary>
    /// <remarks>
    /// This method is primarily intended for building an internal service provider used with
    /// <see cref="Microsoft.EntityFrameworkCore.DbContextOptionsBuilder.UseInternalServiceProvider(System.IServiceProvider)"/>.
    /// </remarks>
    /// <param name="serviceCollection">The <see cref="IServiceCollection"/> to add services to.</param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static IServiceCollection AddEntityFrameworkMimironDb2(this IServiceCollection serviceCollection)
    {
        ArgumentNullException.ThrowIfNull(serviceCollection);

        var builder = new EntityFrameworkServicesBuilder(serviceCollection);
        builder.TryAdd<LoggingDefinitions, MimironDb2LoggingDefinitions>();
        builder.TryAdd<IDatabaseProvider, DatabaseProvider<MimironDb2OptionsExtension>>();
        builder.TryAdd<IQueryContextFactory, MimironDb2QueryContextFactory>();
        builder.TryAdd<IDatabase, MimironDb2Database>();
        builder.TryAdd<ITypeMappingSource, MimironDb2TypeMappingSource>();
        builder.TryAdd<IQueryableMethodTranslatingExpressionVisitorFactory, MimironDb2QueryableMethodTranslatingExpressionVisitorFactory>();
        builder.TryAdd<IShapedQueryCompilingExpressionVisitorFactory, MimironDb2ShapedQueryCompilingExpressionVisitorFactory>();

        // Provider-specific services (Cosmos-style): these are not EF Core interfaces but are required at runtime.
        builder.TryAddProviderSpecificServices(
            services =>
            {
                services.TryAddSingleton<IDbdParser, DbdParser>();
                services.TryAddSingleton<Wdc5FormatOptions, Wdc5FormatOptions>();
                services.TryAddSingleton<IDb2Format, Wdc5Format>();

                services.TryAddSingleton<Db2FkGroupingCache, Db2FkGroupingCache>();

                // DbContext-scoped store: caches parsed DB2 headers / files per table.
                services.TryAddScoped<IMimironDb2Store, MimironDb2Store>();
                services.TryAddScoped<IDb2ModelBinding, Db2ModelBindingProvider>();
            });
        builder.TryAddCoreServices();

        return serviceCollection;
    }
}
