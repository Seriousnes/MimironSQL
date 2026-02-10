using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace MimironSQL.Providers;

/// <summary>
/// Service registration helpers for CASC-based DB2 stream access.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers CASC services required to open DB2 streams from a World of Warcraft installation.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="options">CASC provider options.</param>
    /// <returns>The same <paramref name="services"/> instance to enable chaining.</returns>
    public static IServiceCollection AddCasc(
        this IServiceCollection services,
        CascDb2ProviderOptions options)
        => AddCascInternal(services, configuration: null, options);

    /// <summary>
    /// Registers CASC services required to open DB2 streams from a World of Warcraft installation.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">Configuration used as an additional source for CASC settings.</param>
    /// <returns>The same <paramref name="services"/> instance to enable chaining.</returns>
    public static IServiceCollection AddCasc(
        this IServiceCollection services,
        IConfiguration configuration)
        => AddCascInternal(services, configuration, options: null);

    private static IServiceCollection AddCascInternal(
        IServiceCollection services,
        IConfiguration? configuration,
        CascDb2ProviderOptions? options)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (options is null && configuration is null)
            throw new ArgumentException("Either options or configuration must be provided.");

        var bound = options ?? CascDb2ProviderOptions.FromConfiguration(configuration!);

        const string InstallRootRequiredMessage =
            "WoW install root is required. Configure 'Casc:WowInstallRoot' (e.g. appsettings.json).";

        if (string.IsNullOrWhiteSpace(bound.WowInstallRoot))
            throw new InvalidOperationException(InstallRootRequiredMessage);

        services.TryAddSingleton(bound);

        // Default manifest provider uses WoWDBDefs manifest.json with local-first fallback.
        services.AddHttpClient<WowDb2ManifestProvider>();
        services.TryAddSingleton<IManifestProvider>(sp =>
            new LocalFirstManifestProvider(
                sp.GetRequiredService<WowDb2ManifestProvider>(),
                sp.GetRequiredService<CascDb2ProviderOptions>()));

        services.TryAddSingleton<ICascStorageService, CascStorageService>();

        // CascDBCProvider requires an opened storage instance.
        services.TryAddSingleton(sp =>
        {
            var installRoot = sp.GetRequiredService<CascDb2ProviderOptions>().WowInstallRoot;
            var storageService = sp.GetRequiredService<ICascStorageService>();
            return storageService.OpenInstallRootAsync(installRoot).ConfigureAwait(false).GetAwaiter().GetResult();
        });

        services.TryAddSingleton<CascDBCProvider>();
        services.TryAddSingleton<IDb2StreamProvider>(sp => sp.GetRequiredService<CascDBCProvider>());

        return services;
    }
}
