using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

using NSubstitute;

using Shouldly;

using MimironSQL.Providers;
using MimironSQL.EntityFrameworkCore.Infrastructure;

namespace MimironSQL.EntityFrameworkCore.Tests;

public class MimironDb2OptionsExtensionTests
{
    [Fact]
    public void UseMimironDb2_ShouldDefaultToTracking()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        optionsBuilder.UseMimironDb2(builder => builder.ConfigureProvider(
            providerKey: "Test",
            providerConfigHash: 123,
            applyProviderServices: _ => { }));

        optionsBuilder.Options.FindExtension<CoreOptionsExtension>()
            ?.QueryTrackingBehavior
            .ShouldBe(QueryTrackingBehavior.TrackAll);
    }

    [Fact]
    public void UseMimironDb2_ShouldConfigureOptionsExtension()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        optionsBuilder.UseMimironDb2(builder => builder.ConfigureProvider(
            providerKey: "Test",
            providerConfigHash: 123,
            applyProviderServices: _ => { }));

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();
        extension.ProviderKey.ShouldBe("Test");
        extension.ProviderConfigHash.ShouldBe(123);
    }

    [Fact]
    public void UseMimironDb2_WithConfigureOptions_ShouldInvokeCallback()
    {
        var optionsBuilder = new DbContextOptionsBuilder();
        var callbackInvoked = false;

        optionsBuilder.UseMimironDb2(builder =>
        {
            builder.ShouldNotBeNull();
            callbackInvoked = true;

            builder.ConfigureProvider(
                providerKey: "Test",
                providerConfigHash: 1,
                applyProviderServices: _ => { });
        });

        callbackInvoked.ShouldBeTrue();
    }

    [Fact]
    public void UseMimironDb2_WithNullOptionsBuilder_ShouldThrow()
    {
        DbContextOptionsBuilder optionsBuilder = null!;

        Should.Throw<ArgumentNullException>(() => optionsBuilder.UseMimironDb2(_ => { }));
    }

    [Fact]
    public void UseMimironDb2_WithNullConfigureOptions_ShouldThrow()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        Action<MimironDb2DbContextOptionsBuilder> configureOptions = null!;
        Should.Throw<ArgumentNullException>(() => optionsBuilder.UseMimironDb2(configureOptions));
    }

    [Fact]
    public void UseMimironDb2_WithoutProviderConfiguration_ShouldThrow()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        Should.Throw<InvalidOperationException>(() => optionsBuilder.UseMimironDb2(_ => { }))
            .Message.ShouldContain("providers must be configured");
    }

    [Fact]
    public void Validate_WithoutProviders_ShouldThrow()
    {
        var extension = new MimironDb2OptionsExtension();
        var options = new DbContextOptions<DbContext>();

        Should.Throw<InvalidOperationException>(() => extension.Validate(options))
            .Message.ShouldContain("providers must be configured");
    }

    [Fact]
    public void Validate_WithProviders_ShouldNotThrow()
    {
        var extension = new MimironDb2OptionsExtension().WithProvider(
            providerKey: "Test",
            providerConfigHash: 1,
            applyProviderServices: _ => { });
        var options = new DbContextOptions<DbContext>();

        Should.NotThrow(() => extension.Validate(options));
    }

    [Fact]
    public void Info_ShouldReturnExtensionInfo()
    {
        var extension = new MimironDb2OptionsExtension().WithProvider(
            providerKey: "Test",
            providerConfigHash: 1,
            applyProviderServices: _ => { });

        var info = extension.Info;

        info.ShouldNotBeNull();
        info.IsDatabaseProvider.ShouldBeTrue();
    }

    [Fact]
    public void Info_LogFragment_ShouldContainProviderType()
    {
        var extension = new MimironDb2OptionsExtension().WithProvider(
            providerKey: "Test",
            providerConfigHash: 1,
            applyProviderServices: _ => { });

        var logFragment = extension.Info.LogFragment;

        logFragment.ShouldContain("MimironDb2");
        logFragment.ShouldContain("Provider=");
    }

    [Fact]
    public void Info_LogFragment_WithoutPath_ShouldNotIncludePath()
    {
        var extension = new MimironDb2OptionsExtension();

        var logFragment = extension.Info.LogFragment;

        logFragment.ShouldContain("MimironDb2");
        logFragment.ShouldNotContain("Provider=");
    }

    [Fact]
    public void Info_GetServiceProviderHashCode_ShouldBeConsistent()
    {
        var extension1 = new MimironDb2OptionsExtension().WithProvider("Test", 1, _ => { });
        var extension2 = new MimironDb2OptionsExtension().WithProvider("Test", 1, _ => { });

        var hash1 = extension1.Info.GetServiceProviderHashCode();
        var hash2 = extension2.Info.GetServiceProviderHashCode();

        hash1.ShouldBe(hash2);
    }

    [Fact]
    public void Info_ShouldUseSameServiceProvider_WithSameConfig_ShouldReturnTrue()
    {
        var extension1 = new MimironDb2OptionsExtension().WithProvider("Test", 1, _ => { });
        var extension2 = new MimironDb2OptionsExtension().WithProvider("Test", 1, _ => { });

        var result = extension1.Info.ShouldUseSameServiceProvider(extension2.Info);

        result.ShouldBeTrue();
    }

    [Fact]
    public void Info_ShouldUseSameServiceProvider_WithDifferentProviderInstances_ShouldReturnFalse()
    {
        var extension1 = new MimironDb2OptionsExtension().WithProvider("Test", 1, _ => { });
        var extension2 = new MimironDb2OptionsExtension().WithProvider("Test", 2, _ => { });

        var result = extension1.Info.ShouldUseSameServiceProvider(extension2.Info);

        result.ShouldBeFalse();
    }

    [Fact]
    public void Info_PopulateDebugInfo_ShouldAddProviderTypes()
    {
        var extension = new MimironDb2OptionsExtension().WithProvider("Test", 1, _ => { });
        var debugInfo = new Dictionary<string, string>();

        extension.Info.PopulateDebugInfo(debugInfo);

        debugInfo.ShouldContainKey("MimironDb2:Provider");
        debugInfo.ShouldContainKey("MimironDb2:ProviderConfigHash");
    }

    [Fact]
    public void ApplyServices_ShouldNotThrow()
    {
        var extension = new MimironDb2OptionsExtension();
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();

        Should.NotThrow(() => extension.ApplyServices(services));
    }

    [Fact]
    public void MimironDb2DbContextOptionsBuilder_WithNullOptionsBuilder_ShouldThrow()
    {
        DbContextOptionsBuilder optionsBuilder = null!;

        Should.Throw<ArgumentNullException>(() => new MimironDb2DbContextOptionsBuilder(optionsBuilder));
    }

    [Fact]
    public void MimironDb2DbContextOptionsBuilder_ShouldStoreOptionsBuilder()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        var builder = new MimironDb2DbContextOptionsBuilder(optionsBuilder);

        builder.ShouldNotBeNull();
    }

    [Fact]
    public void MultipleProviderConfigurations_LastOneWins()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        optionsBuilder.UseMimironDb2(b => b.ConfigureProvider("First", 1, _ => { }));
        Should.Throw<InvalidOperationException>(() =>
            optionsBuilder.UseMimironDb2(b => b.ConfigureProvider("Second", 2, _ => { })));
    }

    [Fact]
    public void UseMimironDb2_ShouldReturnOptionsBuilderForChaining()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        var result = optionsBuilder.UseMimironDb2(b => b.ConfigureProvider("Test", 1, _ => { }));

        result.ShouldBeSameAs(optionsBuilder);
    }
}
