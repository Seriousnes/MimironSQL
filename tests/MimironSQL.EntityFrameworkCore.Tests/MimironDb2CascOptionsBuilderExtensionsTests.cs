using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

using NSubstitute;

using Shouldly;

using MimironSQL.EntityFrameworkCore.Infrastructure;
using MimironSQL.Providers;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class MimironDb2CascOptionsBuilderExtensionsTests : IDisposable
{
    private readonly string _fakeWowRoot;

    public MimironDb2CascOptionsBuilderExtensionsTests()
    {
        _fakeWowRoot = Path.Combine(Path.GetTempPath(), $"MimironTest_{Guid.NewGuid():N}");
        CreateFakeWowLayout(_fakeWowRoot);
    }

    public void Dispose()
    {
        if (Directory.Exists(_fakeWowRoot))
        {
            Directory.Delete(_fakeWowRoot, recursive: true);
        }
    }

    [Fact]
    public void UseCasc_WithFactories_ShouldResolve_CustomProviders()
    {
        var expectedDbdProvider = Substitute.For<IDbdProvider>();
        var expectedManifestProvider = Substitute.For<IManifestProvider>();

        var optionsBuilder = new DbContextOptionsBuilder();
        optionsBuilder.UseMimironDb2ForTests(o => o.UseCasc(casc =>
        {
            casc.WowInstallRoot = _fakeWowRoot;
            casc.DbdProviderFactory = _ => expectedDbdProvider;
            casc.ManifestProviderFactory = _ => expectedManifestProvider;
        }));

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();

        var services = new ServiceCollection();
        extension!.ApplyServices(services);

        using var serviceProvider = services.BuildServiceProvider();

        serviceProvider.GetRequiredService<IDbdProvider>().ShouldBeSameAs(expectedDbdProvider);
        serviceProvider.GetRequiredService<IManifestProvider>().ShouldBeSameAs(expectedManifestProvider);
    }

    [Fact]
    public void UseCasc_WithInstances_ShouldResolve_CustomProviders()
    {
        var expectedDbdProvider = Substitute.For<IDbdProvider>();
        var expectedManifestProvider = Substitute.For<IManifestProvider>();

        var optionsBuilder = new DbContextOptionsBuilder();
        optionsBuilder.UseMimironDb2ForTests(o => o.UseCasc(casc =>
        {
            casc.WowInstallRoot = _fakeWowRoot;
            casc.DbdProvider = expectedDbdProvider;
            casc.ManifestProvider = expectedManifestProvider;
        }));

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();

        var services = new ServiceCollection();
        extension!.ApplyServices(services);

        using var serviceProvider = services.BuildServiceProvider();

        serviceProvider.GetRequiredService<IDbdProvider>().ShouldBeSameAs(expectedDbdProvider);
        serviceProvider.GetRequiredService<IManifestProvider>().ShouldBeSameAs(expectedManifestProvider);
    }

    [Fact]
    public void UseCasc_WithoutDbdDefinitions_AndWithoutCustomDbdProvider_ShouldThrow()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        Should.Throw<InvalidOperationException>(() =>
            optionsBuilder.UseMimironDb2ForTests(o => o
                .UseCasc()
                .WithWowInstallRoot(_fakeWowRoot)
                .Apply()));
    }

    [Fact]
    public void UseCasc_WithManifestProviderType_ShouldRegister_OnlyOneManifestProvider()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        optionsBuilder.UseMimironDb2ForTests(o => o.UseCasc(casc =>
        {
            casc.WowInstallRoot = _fakeWowRoot;
            casc.DbdProviderFactory = _ => Substitute.For<IDbdProvider>();
            casc.WithManifestProvider<TestManifestProvider>();
        }));

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();

        var services = new ServiceCollection();
        extension!.ApplyServices(services);

        services.Count(d => d.ServiceType == typeof(IManifestProvider)).ShouldBe(1);

        using var serviceProvider = services.BuildServiceProvider();
        serviceProvider.GetRequiredService<IManifestProvider>().ShouldBeOfType<TestManifestProvider>();
    }

    [Fact]
    public void UseCasc_AutoDetectsWowVersion_FromBuildInfo()
    {
        var optionsBuilder = new DbContextOptionsBuilder();
        optionsBuilder.UseMimironDb2(o => o
            .UseCasc(casc =>
            {
                casc.WowInstallRoot = _fakeWowRoot;
                casc.DbdProviderFactory = _ => Substitute.For<IDbdProvider>();
            }));

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();
        extension!.WowVersion.ShouldBe("11.0.7.99999");
    }

    [Fact]
    public void UseCasc_ExplicitWowVersion_OverridesAutoDetected()
    {
        var optionsBuilder = new DbContextOptionsBuilder();
        optionsBuilder.UseMimironDb2(o =>
        {
            o.UseCasc(casc =>
            {
                casc.WowInstallRoot = _fakeWowRoot;
                casc.DbdProviderFactory = _ => Substitute.For<IDbdProvider>();
            });
            o.WithWowVersion("99.0.0.12345");
        });

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();
        extension!.WowVersion.ShouldBe("99.0.0.12345");
    }

    private static void CreateFakeWowLayout(string root, string version = "11.0.7.99999", string product = "wow")
    {
        var flavorDir = Path.Combine(root, "_retail_");
        Directory.CreateDirectory(Path.Combine(flavorDir, "Data", "data"));
        Directory.CreateDirectory(Path.Combine(flavorDir, "Data", "config"));

        File.WriteAllText(Path.Combine(flavorDir, ".flavor.info"), product);
        File.WriteAllText(
            Path.Combine(root, ".build.info"),
            $"Branch!STRING:0|Active!DEC:1|Build Key!HEX:16|Version!STRING:0|Product!STRING:0\nus|1|aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1|{version}|{product}\n");
    }

    private sealed class TestManifestProvider : IManifestProvider
    {
        public Task EnsureManifestExistsAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task<int?> TryResolveDb2FileDataIdAsync(string db2NameOrPath, CancellationToken cancellationToken = default)
            => Task.FromResult<int?>(null);
    }
}
