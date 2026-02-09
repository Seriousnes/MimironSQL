using Microsoft.EntityFrameworkCore;

using MimironSQL.EntityFrameworkCore;
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

        var optionsBuilder = new DbContextOptionsBuilder<WoWDb2Context>();
        optionsBuilder.UseMimironDb2(o => o.UseCascNet(
            wowInstallRoot: wowInstallRoot,
            dbdDefinitionsDirectory: testDataDir,
            configureWowDb2Manifest: m =>
            {
                m.CacheDirectory = testDataDir;
                m.AssetName = "manifest.json";
            }));

        using var context = new WoWDb2Context(optionsBuilder.Options);

        var results = context.Map
            .Include(x => x.MapChallengeModes)
            .Where(x => x.MapChallengeModes.Count > 0)
            .Take(10).ToList();
        results.Count.ShouldBeGreaterThan(0);
        results.Any(x => x.Id > 0).ShouldBeTrue();
        results.Any(x => !string.IsNullOrWhiteSpace(x.Directory)).ShouldBeTrue();
    }
}