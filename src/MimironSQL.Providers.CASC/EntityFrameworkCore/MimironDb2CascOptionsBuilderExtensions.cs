using Microsoft.Extensions.DependencyInjection;

using MimironSQL.EntityFrameworkCore;

namespace MimironSQL.Providers;

public static class MimironDb2CascOptionsBuilderExtensions
{
    public static MimironDb2DbContextOptionsBuilder UseCascNet(
        this MimironDb2DbContextOptionsBuilder builder,
        string wowInstallRoot,
        string dbdDefinitionsDirectory,
        Action<CascStorageOptions>? configureStorage = null,
        Action<WowDb2ManifestOptions>? configureWowDb2Manifest = null,
        Action<CascNetOptions>? configureCascNet = null)
        => builder.UseCascNet(
            wowInstallRoot,
            new FileSystemDbdProviderOptions(dbdDefinitionsDirectory),
            configureStorage,
            configureWowDb2Manifest,
            configureCascNet);

    public static MimironDb2DbContextOptionsBuilder UseCascNet(
        this MimironDb2DbContextOptionsBuilder builder,
        string wowInstallRoot,
        FileSystemDbdProviderOptions dbdOptions,
        Action<CascStorageOptions>? configureStorage = null,
        Action<WowDb2ManifestOptions>? configureWowDb2Manifest = null,
        Action<CascNetOptions>? configureCascNet = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(wowInstallRoot);
        ArgumentNullException.ThrowIfNull(dbdOptions);

        var manifestOptionsSnapshot = new WowDb2ManifestOptions();
        configureWowDb2Manifest?.Invoke(manifestOptionsSnapshot);

        var configHash = HashCode.Combine(
            wowInstallRoot,
            dbdOptions.DefinitionsDirectory,
            manifestOptionsSnapshot.CacheDirectory,
            manifestOptionsSnapshot.AssetName);

        return builder.ConfigureProvider(
            providerKey: "CASC",
            providerConfigHash: configHash,
            applyProviderServices: services =>
            {
                services.AddCascNet(
                    configureStorage: configureStorage,
                    configureWowDb2Manifest: configureWowDb2Manifest,
                    configureCascNet: o =>
                    {
                        o.WowInstallRoot = wowInstallRoot;
                        configureCascNet?.Invoke(o);
                    });

                services.AddSingleton(dbdOptions);
                services.AddSingleton<IDbdProvider, FileSystemDbdProvider>();
            });
    }
}
