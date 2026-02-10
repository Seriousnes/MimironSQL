using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using MimironSQL.EntityFrameworkCore;

namespace MimironSQL.Providers;

/// <summary>
/// Provider configuration extensions for reading DB2 streams from a World of Warcraft installation via CASC.
/// </summary>
public static class MimironDb2CascOptionsBuilderExtensions
{
    /// <summary>
    /// Begins configuring the CASC provider using a fluent builder.
    /// </summary>
    /// <param name="builder">The provider options builder.</param>
    /// <returns>A fluent CASC provider builder. Call <see cref="CascDb2ProviderBuilder.Apply"/> to apply.</returns>
    public static CascDb2ProviderBuilder UseCasc(this IMimironDb2DbContextOptionsBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return new CascDb2ProviderBuilder(builder);
    }

    /// <summary>
    /// Configures the CASC provider using a configuration callback.
    /// </summary>
    /// <param name="builder">The provider options builder.</param>
    /// <param name="configure">Callback used to configure the CASC provider.</param>
    /// <returns>The same <paramref name="builder"/> instance to enable chaining.</returns>
    public static IMimironDb2DbContextOptionsBuilder UseCasc(
        this IMimironDb2DbContextOptionsBuilder builder,
        Action<CascDb2ProviderBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var casc = builder.UseCasc();
        configure(casc);
        return casc.Apply();
    }

    /// <summary>
    /// Configures the CASC provider using configuration binding and applies it immediately.
    /// </summary>
    /// <param name="builder">The provider options builder.</param>
    /// <param name="configuration">Configuration used to bind <see cref="CascDb2ProviderOptions"/>.</param>
    /// <returns>The same <paramref name="builder"/> instance to enable chaining.</returns>
    public static IMimironDb2DbContextOptionsBuilder UseCasc(
        this IMimironDb2DbContextOptionsBuilder builder,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configuration);

        var bound = BindOptions(configuration);
        return builder.UseCasc(casc =>
        {
            casc.WowInstallRoot = bound.WowInstallRoot;
            casc.DbdDefinitionsDirectory = bound.DbdDefinitionsDirectory;
            casc.ManifestCacheDirectory = bound.ManifestCacheDirectory;
            casc.ManifestAssetName = bound.ManifestAssetName;
        });
    }

    private static CascDb2ProviderOptions BindOptions(IConfiguration configuration)
    {
        var casc = configuration.GetSection("Casc");

        static string? ReadString(IConfigurationSection section, IConfiguration root, string key)
            => section[key]?.Trim() is { Length: > 0 } v ? v : (root[key]?.Trim() is { Length: > 0 } r ? r : null);

        var wowInstallRoot = ReadString(casc, configuration, "WowInstallRoot") ?? string.Empty;
        var dbdDefsDir = ReadString(casc, configuration, "DbdDefinitionsDirectory");
        var cacheDir = ReadString(casc, configuration, "ManifestCacheDirectory");

        var assetName = casc["ManifestAssetName"]?.Trim();
        if (string.IsNullOrWhiteSpace(assetName))
            assetName = configuration["ManifestAssetName"]?.Trim();
        if (string.IsNullOrWhiteSpace(assetName))
            assetName = "manifest.json";

        return new CascDb2ProviderOptions
        {
            WowInstallRoot = wowInstallRoot,
            DbdDefinitionsDirectory = dbdDefsDir,
            ManifestCacheDirectory = cacheDir,
            ManifestAssetName = assetName,
        };
    }

    /// <summary>
    /// Fluent configuration builder for the CASC DB2 provider.
    /// </summary>
    public sealed class CascDb2ProviderBuilder
    {
        private readonly IMimironDb2DbContextOptionsBuilder _builder;

        private Type? _dbdProviderType;
        private Type? _manifestProviderType;

        internal CascDb2ProviderBuilder(IMimironDb2DbContextOptionsBuilder builder)
        {
            _builder = builder;
        }

        /// <summary>
        /// Root directory of the World of Warcraft installation.
        /// </summary>
        public string WowInstallRoot { get; set; } = string.Empty;

        /// <summary>
        /// Directory containing WoWDBDefs <c>.dbd</c> files.
        /// When set, the file-system DBD provider is registered unless a custom DBD provider is configured.
        /// </summary>
        public string? DbdDefinitionsDirectory { get; set; }

        /// <summary>
        /// Optional cache directory for the WoWDBDefs DB2 manifest.
        /// </summary>
        public string? ManifestCacheDirectory { get; set; }

        /// <summary>
        /// The manifest asset name (default: <c>manifest.json</c>).
        /// </summary>
        public string ManifestAssetName { get; set; } = "manifest.json";

        /// <summary>
        /// Optional explicit DBD provider instance to register.
        /// </summary>
        public IDbdProvider? DbdProvider { get; set; }

        /// <summary>
        /// Optional DBD provider factory to register via DI.
        /// </summary>
        public Func<IServiceProvider, IDbdProvider>? DbdProviderFactory { get; set; }

        /// <summary>
        /// Optional explicit manifest provider instance to register.
        /// </summary>
        public IManifestProvider? ManifestProvider { get; set; }

        /// <summary>
        /// Optional manifest provider factory to register via DI.
        /// </summary>
        public Func<IServiceProvider, IManifestProvider>? ManifestProviderFactory { get; set; }

        /// <summary>
        /// Sets the WoW install root.
        /// </summary>
        public CascDb2ProviderBuilder WithWowInstallRoot(string wowInstallRoot)
        {
            WowInstallRoot = wowInstallRoot;
            return this;
        }

        /// <summary>
        /// Sets the directory containing WoWDBDefs <c>.dbd</c> files.
        /// </summary>
        public CascDb2ProviderBuilder WithDbdDefinitions(string dbdDefinitionsDirectory)
        {
            DbdDefinitionsDirectory = dbdDefinitionsDirectory;
            _dbdProviderType = null;
            DbdProvider = null;
            DbdProviderFactory = null;
            return this;
        }

        /// <summary>
        /// Configures a custom DBD provider type.
        /// </summary>
        public CascDb2ProviderBuilder WithDbdDefinitions<TDbdProvider>() where TDbdProvider : class, IDbdProvider
            => WithDbdProvider<TDbdProvider>();

        /// <summary>
        /// Configures a custom DBD provider type.
        /// </summary>
        public CascDb2ProviderBuilder WithDbdProvider<TDbdProvider>() where TDbdProvider : class, IDbdProvider
        {
            _dbdProviderType = typeof(TDbdProvider);
            DbdDefinitionsDirectory = null;
            DbdProvider = null;
            DbdProviderFactory = null;
            return this;
        }

        /// <summary>
        /// Configures the manifest cache directory and optional asset name.
        /// </summary>
        public CascDb2ProviderBuilder WithManifest(string manifestCacheDirectory, string? manifestAssetName = null)
        {
            ManifestCacheDirectory = manifestCacheDirectory;
            if (!string.IsNullOrWhiteSpace(manifestAssetName))
                ManifestAssetName = manifestAssetName;

            return this;
        }

        /// <summary>
        /// Configures a custom manifest provider type.
        /// </summary>
        public CascDb2ProviderBuilder WithManifest<TManifestProvider>() where TManifestProvider : class, IManifestProvider
            => WithManifestProvider<TManifestProvider>();

        /// <summary>
        /// Configures a custom manifest provider type.
        /// </summary>
        public CascDb2ProviderBuilder WithManifestProvider<TManifestProvider>() where TManifestProvider : class, IManifestProvider
        {
            _manifestProviderType = typeof(TManifestProvider);
            ManifestProvider = null;
            ManifestProviderFactory = null;
            return this;
        }

        /// <summary>
        /// Applies the configured CASC provider settings to the underlying provider options builder.
        /// </summary>
        /// <returns>The underlying provider options builder.</returns>
        public IMimironDb2DbContextOptionsBuilder Apply()
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(WowInstallRoot);

            var hasCustomDbdProvider = _dbdProviderType is not null || DbdProvider is not null || DbdProviderFactory is not null;
            if (!hasCustomDbdProvider && string.IsNullOrWhiteSpace(DbdDefinitionsDirectory))
            {
                throw new InvalidOperationException(
                    "DBD definitions are required. Configure 'Casc:DbdDefinitionsDirectory' (e.g. appsettings.json), call WithDbdDefinitions(...), or configure a custom IDbdProvider.");
            }

            var cascOptions = new CascDb2ProviderOptions
            {
                WowInstallRoot = WowInstallRoot,
                DbdDefinitionsDirectory = DbdDefinitionsDirectory,
                ManifestCacheDirectory = ManifestCacheDirectory,
                ManifestAssetName = ManifestAssetName,
            };

            var configHash = HashCode.Combine(
                cascOptions.WowInstallRoot,
                hasCustomDbdProvider ? (_dbdProviderType?.FullName ?? DbdProvider?.GetType().FullName ?? "factory") : cascOptions.DbdDefinitionsDirectory,
                _manifestProviderType?.FullName ?? ManifestProvider?.GetType().FullName ?? (ManifestProviderFactory is not null ? "factory" : null),
                cascOptions.ManifestCacheDirectory,
                cascOptions.ManifestAssetName);

            return _builder.ConfigureProvider(
                providerKey: "CASC",
                providerConfigHash: configHash,
                applyProviderServices: services =>
                {
                    RegisterManifestProvider(services);
                    services.AddCasc(cascOptions);
                    RegisterDbdProvider(services);
                });
        }

        private void RegisterDbdProvider(IServiceCollection services)
        {
            if (_dbdProviderType is not null)
            {
                if (!typeof(IDbdProvider).IsAssignableFrom(_dbdProviderType))
                    throw new InvalidOperationException($"Configured DBD provider type '{_dbdProviderType}' does not implement IDbdProvider.");

                services.AddSingleton(typeof(IDbdProvider), _dbdProviderType);
                return;
            }

            if (DbdProvider is not null)
            {
                services.AddSingleton<IDbdProvider>(DbdProvider);
                return;
            }

            if (DbdProviderFactory is not null)
            {
                services.AddSingleton<IDbdProvider>(sp => DbdProviderFactory(sp));
                return;
            }

            if (string.IsNullOrWhiteSpace(DbdDefinitionsDirectory))
                throw new InvalidOperationException("DBD definitions directory is required.");

            services.AddSingleton(new FileSystemDbdProviderOptions(DbdDefinitionsDirectory));
            services.AddSingleton<IDbdProvider, FileSystemDbdProvider>();
        }

        private void RegisterManifestProvider(IServiceCollection services)
        {
            if (_manifestProviderType is not null)
            {
                if (!typeof(IManifestProvider).IsAssignableFrom(_manifestProviderType))
                    throw new InvalidOperationException($"Configured manifest provider type '{_manifestProviderType}' does not implement IManifestProvider.");

                services.AddSingleton(typeof(IManifestProvider), _manifestProviderType);
                return;
            }

            if (ManifestProvider is not null)
            {
                services.AddSingleton<IManifestProvider>(ManifestProvider);
                return;
            }

            if (ManifestProviderFactory is not null)
            {
                services.AddSingleton<IManifestProvider>(sp => ManifestProviderFactory(sp));
            }
        }
    }
}
