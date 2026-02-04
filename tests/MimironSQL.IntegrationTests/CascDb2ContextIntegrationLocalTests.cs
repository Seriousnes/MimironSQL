using Microsoft.Extensions.Options;

using MimironSQL.Db2.Query;
using MimironSQL.Formats.Wdc5;
using MimironSQL.IntegrationTests.Helpers;
using MimironSQL.Providers;

using Shouldly;

namespace MimironSQL.IntegrationTests;

public sealed class CascDb2ContextIntegrationLocalTests
{
    [LocalCascFact]
    public async Task Can_query_db2context_using_casc_db2_provider()
    {
        LocalEnvLocal.TryGetWowInstallRoot(out var wowInstallRoot).ShouldBeTrue();
        Directory.Exists(wowInstallRoot).ShouldBeTrue();

        var testDataDir = TestDataPaths.GetTestDataDirectory();
        Directory.Exists(testDataDir).ShouldBeTrue();

        var manifestPath = Path.Combine(testDataDir, "manifest.json");
        File.Exists(manifestPath).ShouldBeTrue();

        var options = new WowDb2ManifestOptions
        {
            CacheDirectory = testDataDir,
            AssetName = "manifest.json",
        };

        using var httpClient = new HttpClient();
        var wowDb2ManifestProvider = new WowDb2ManifestProvider(httpClient, Options.Create(options));
        var manifestProvider = new LocalFirstManifestProvider(wowDb2ManifestProvider, Options.Create(options));

        await manifestProvider.EnsureManifestExistsAsync();

        var storage = await CascStorage.OpenInstallRootAsync(wowInstallRoot);
        var db2Provider = new CascDBCProvider(storage, manifestProvider);

        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new WoWDb2Context(dbdProvider, db2Provider, Wdc5Format.Instance);        
        context.EnsureModelCreated();

        var results = context.Map
            .Include(x => x.MapChallengeModes)
            .Where(x => x.MapChallengeModes.Count > 0)
            .Take(10).ToList();
        results.Count.ShouldBeGreaterThan(0);
        results.Any(x => x.Id > 0).ShouldBeTrue();
        results.Any(x => !string.IsNullOrWhiteSpace(x.Directory)).ShouldBeTrue();
    }
}