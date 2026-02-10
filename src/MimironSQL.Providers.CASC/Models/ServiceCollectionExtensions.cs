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
        => AddCasc(services, configuration: null, options);

    /// <summary>
    /// Registers CASC services required to open DB2 streams from a World of Warcraft installation.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">Configuration used as an additional source for CASC settings.</param>
    /// <returns>The same <paramref name="services"/> instance to enable chaining.</returns>
    public static IServiceCollection AddCasc(
        this IServiceCollection services,
        IConfiguration configuration)
        => AddCasc(services, configuration, options: null);

    private static IServiceCollection AddCasc(
        IServiceCollection services,
        IConfiguration? configuration,
        CascDb2ProviderOptions? options)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (options is null && configuration is null)
            throw new ArgumentException("Either options or configuration must be provided.");

        var bound = options ?? BindOptions(configuration!);

        if (string.IsNullOrWhiteSpace(bound.WowInstallRoot))
            throw new InvalidOperationException("WoW install root is required. Configure 'Casc:WowInstallRoot'.");

        var hasManifestProvider = services.Any(x => x.ServiceType == typeof(IManifestProvider));
        if (!hasManifestProvider && string.IsNullOrWhiteSpace(bound.ManifestDirectory))
            throw new ArgumentException("Manifest directory is required. Configure 'Casc:ManifestDirectory'.", options is null ? nameof(configuration) : nameof(options));

        services.TryAddSingleton(bound);

        services.TryAddSingleton<IWowBuildIdentityProvider, WowBuildIdentityProvider>();
        services.TryAddSingleton<IManifestProvider, FileSystemManifestProvider>();
        services.TryAddSingleton<CascStorageService>();
        services.TryAddSingleton<IDb2StreamProvider>(sp => sp.GetRequiredService<CascStorageService>());
        
        return services;
    }

    private static CascDb2ProviderOptions BindOptions(IConfiguration configuration)
    {
        var casc = configuration.GetSection("Casc");

        static string? ReadString(IConfigurationSection section, IConfiguration root, string key)
            => section[key]?.Trim() is { Length: > 0 } v ? v : (root[key]?.Trim() is { Length: > 0 } r ? r : null);

        var wowInstallRoot = ReadString(casc, configuration, "WowInstallRoot") ?? string.Empty;
        var dbdDefsDir = ReadString(casc, configuration, "DbdDefinitionsDirectory");
        var manifestDir = ReadString(casc, configuration, "ManifestDirectory") ?? string.Empty;

        var assetName = casc["ManifestAssetName"]?.Trim() ?? configuration["ManifestAssetName"]?.Trim() ?? "manifest.json";

        return new CascDb2ProviderOptions
        {
            WowInstallRoot = wowInstallRoot,
            DbdDefinitionsDirectory = dbdDefsDir,
            ManifestDirectory = manifestDir,
            ManifestAssetName = assetName,
        };
    }
}
