using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace MimironSQL.Providers;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddCascNet(
        this IServiceCollection services,
        Action<CascStorageOptions>? configureStorage = null,
        Action<WowListfileOptions>? configureWowListfile = null,
        Action<WowDb2ManifestOptions>? configureWowDb2Manifest = null,
        Action<CascNetOptions>? configureCascNet = null)
        => AddCascNetInternal(services, configuration: null, configureStorage, configureWowListfile, configureWowDb2Manifest, configureCascNet);

    public static IServiceCollection AddCascNet(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<CascStorageOptions>? configureStorage = null,
        Action<WowListfileOptions>? configureWowListfile = null,
        Action<WowDb2ManifestOptions>? configureWowDb2Manifest = null,
        Action<CascNetOptions>? configureCascNet = null)
        => AddCascNetInternal(services, configuration, configureStorage, configureWowListfile, configureWowDb2Manifest, configureCascNet);

    private static IServiceCollection AddCascNetInternal(
        IServiceCollection services,
        IConfiguration? configuration,
        Action<CascStorageOptions>? configureStorage,
        Action<WowListfileOptions>? configureWowListfile,
        Action<WowDb2ManifestOptions>? configureWowDb2Manifest,
        Action<CascNetOptions>? configureCascNet)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions<CascNetOptions>();
        if (configureCascNet is not null)
            services.Configure(configureCascNet);

        var cascNetOptions = new CascNetOptions
        {
            WowInstallRoot = configuration?["CascNet:WowInstallRoot"]?.Trim() ?? string.Empty,
        };

        var envInstallRoot = Environment.GetEnvironmentVariable("CascNet__WowInstallRoot");
        if (!string.IsNullOrWhiteSpace(envInstallRoot))
            cascNetOptions.WowInstallRoot = envInstallRoot.Trim();

        configureCascNet?.Invoke(cascNetOptions);

        if (string.IsNullOrWhiteSpace(cascNetOptions.WowInstallRoot))
        {
            throw new InvalidOperationException(
                "WoW install root is required. Configure 'CascNet:WowInstallRoot' (e.g. appsettings.json) or set env var 'CascNet__WowInstallRoot'.");
        }

        services.PostConfigure<CascNetOptions>(o => o.WowInstallRoot = cascNetOptions.WowInstallRoot);

        services.AddOptions<CascStorageOptions>();
        if (configureStorage is not null)
            services.Configure(configureStorage);

        services.AddOptions<WowListfileOptions>();
        if (configureWowListfile is not null)
            services.Configure(configureWowListfile);

        services.AddOptions<WowDb2ManifestOptions>();
        if (configureWowDb2Manifest is not null)
            services.Configure(configureWowDb2Manifest);

        // Default manifest provider uses WoWDBDefs manifest.json with local-first fallback.
        services.AddHttpClient<WowDb2ManifestProvider>();
        services.TryAddSingleton<IManifestProvider>(sp =>
            new LocalFirstManifestProvider(
                sp.GetRequiredService<WowDb2ManifestProvider>(),
                sp.GetRequiredService<IOptions<WowDb2ManifestOptions>>()));

        services.TryAddSingleton<ICascStorageService, CascStorageService>();

        // CascDBCProvider requires an opened storage instance.
        services.TryAddSingleton(sp =>
        {
            var installRoot = sp.GetRequiredService<IOptions<CascNetOptions>>().Value.WowInstallRoot;
            var storageService = sp.GetRequiredService<ICascStorageService>();
            return storageService.OpenInstallRootAsync(installRoot).GetAwaiter().GetResult();
        });

        services.TryAddSingleton<CascDBCProvider>();
        services.TryAddSingleton<IDb2StreamProvider>(sp => sp.GetRequiredService<CascDBCProvider>());

        return services;
    }
}
