using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

using MimironSQL.Dbd;
using MimironSQL.IntegrationTests.Helpers;
using MimironSQL.Providers;

using Shouldly;

namespace MimironSQL.IntegrationTests;

public sealed class CascDb2ContextIntegrationLocalUseCascConfigurationTests(CascDb2ContextIntegrationLocalUseCascConfigurationTestsFixture fixture) : IClassFixture<CascDb2ContextIntegrationLocalUseCascConfigurationTestsFixture>
{
    private WoWDb2Context context => fixture.Context;

    [LocalCascFact]
    public void Can_query_db2context_using_casc_db2_provider_with_delegate_and_di_factories()
    {
        var results = context.Map
            .Include(x => x.MapChallengeModes)
            .Where(x => x.MapChallengeModes.Count > 0)
            .Take(10)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.Any(x => x.Id > 0).ShouldBeTrue();
        results.Any(x => !string.IsNullOrWhiteSpace(x.Directory)).ShouldBeTrue();
    }

    [LocalCascFact]
    public void Can_query_db2context_using_casc_db2_provider_with_configuration_binding()
    {
        var results = context.Map
            .Where(x => x.Id > 0)
            .Take(10)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
    }    
}

public class CascDb2ContextIntegrationLocalUseCascConfigurationTestsFixture
{
    public WoWDb2Context Context { get; }

    public CascDb2ContextIntegrationLocalUseCascConfigurationTestsFixture()
    {
        LocalEnvLocal.TryGetWowInstallRoot(out var wowInstallRoot).ShouldBeTrue();
        Directory.Exists(wowInstallRoot).ShouldBeTrue();

        var testDataDir = TestDataPaths.GetTestDataDirectory();
        Directory.Exists(testDataDir).ShouldBeTrue();

        var manifestPath = Path.Combine(testDataDir, "manifest.json");
        File.Exists(manifestPath).ShouldBeTrue();

        var tactKeyFilePath = Path.Combine(testDataDir, "WoW.txt");
        File.Exists(tactKeyFilePath).ShouldBeTrue();

        var optionsBuilder = new DbContextOptionsBuilder<WoWDb2Context>();
        optionsBuilder.UseMimironDb2ForTests(o => o.UseCasc(casc =>
        {
            casc.WowInstallRoot = wowInstallRoot;

            casc.ManifestProvider = new JsonFileManifestProvider(manifestPath);

            casc.DbdProviderFactory = sp =>
                new FileSystemDbdProvider(
                    new FileSystemDbdProviderOptions(Path.Combine(testDataDir, "definitions")),
                    sp.GetRequiredService<IDbdParser>());

            casc.TactKeyFilePath = tactKeyFilePath;
        }));

        Context = new WoWDb2Context(optionsBuilder.Options);
    }
}
