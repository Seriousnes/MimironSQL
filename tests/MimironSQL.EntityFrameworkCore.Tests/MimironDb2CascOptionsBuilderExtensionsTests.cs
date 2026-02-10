using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

using NSubstitute;

using Shouldly;

using MimironSQL.EntityFrameworkCore.Infrastructure;
using MimironSQL.Providers;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class MimironDb2CascOptionsBuilderExtensionsTests
{
    [Fact]
    public void UseCasc_WithFactories_ShouldResolve_CustomProviders()
    {
        var expectedDbdProvider = Substitute.For<IDbdProvider>();
        var expectedManifestProvider = Substitute.For<IManifestProvider>();

        var optionsBuilder = new DbContextOptionsBuilder();
        optionsBuilder.UseMimironDb2(o => o.UseCasc(casc =>
        {
            casc.WowInstallRoot = "X:\\World of Warcraft";
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
        optionsBuilder.UseMimironDb2(o => o.UseCasc(casc =>
        {
            casc.WowInstallRoot = "X:\\World of Warcraft";
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
            optionsBuilder.UseMimironDb2(o => o
                .UseCasc()
                .WithWowInstallRoot("X:\\World of Warcraft")
                .Apply()));
    }

    [Fact]
    public void UseCasc_WithManifestProviderType_ShouldRegister_OnlyOneManifestProvider()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        optionsBuilder.UseMimironDb2(o => o.UseCasc(casc =>
        {
            casc.WowInstallRoot = "X:\\World of Warcraft";
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

    private sealed class TestManifestProvider : IManifestProvider
    {
        public Task EnsureManifestExistsAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task<int?> TryResolveDb2FileDataIdAsync(string db2NameOrPath, CancellationToken cancellationToken = default)
            => Task.FromResult<int?>(null);
    }
}
