using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using NSubstitute;
using Shouldly;
using MimironSQL.Providers;

namespace MimironSQL.EntityFrameworkCore.Tests;

public class MimironDb2OptionsExtensionTests
{
    [Fact]
    public void UseMimironDb2_ShouldDefaultToNoTracking()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        var db2Provider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        var tactKeyProvider = Substitute.For<ITactKeyProvider>();

        optionsBuilder.UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider);

        optionsBuilder.Options.FindExtension<CoreOptionsExtension>()
            ?.QueryTrackingBehavior
            .ShouldBe(QueryTrackingBehavior.NoTracking);
    }

    [Fact]
    public void UseMimironDb2_ShouldConfigureOptionsExtension()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        var db2Provider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        var tactKeyProvider = Substitute.For<ITactKeyProvider>();

        optionsBuilder.UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider);

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();
        extension.Db2StreamProvider.ShouldBeSameAs(db2Provider);
        extension.DbdProvider.ShouldBeSameAs(dbdProvider);
        extension.TactKeyProvider.ShouldBeSameAs(tactKeyProvider);
    }

    [Fact]
    public void UseMimironDb2_WithConfigureOptions_ShouldInvokeCallback()
    {
        var optionsBuilder = new DbContextOptionsBuilder();
        var callbackInvoked = false;

        var db2Provider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        var tactKeyProvider = Substitute.For<ITactKeyProvider>();

        optionsBuilder.UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider, configureOptions: builder =>
        {
            builder.ShouldNotBeNull();
            callbackInvoked = true;
        });

        callbackInvoked.ShouldBeTrue();
    }

    [Fact]
    public void UseMimironDb2_WithNullOptionsBuilder_ShouldThrow()
    {
        DbContextOptionsBuilder optionsBuilder = null!;

        var db2Provider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        var tactKeyProvider = Substitute.For<ITactKeyProvider>();

        Should.Throw<ArgumentNullException>(() => optionsBuilder.UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider));
    }

    [Fact]
    public void UseMimironDb2_WithNullDb2Provider_ShouldThrow()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        var dbdProvider = Substitute.For<IDbdProvider>();
        var tactKeyProvider = Substitute.For<ITactKeyProvider>();

        Should.Throw<ArgumentNullException>(() => optionsBuilder.UseMimironDb2(null!, dbdProvider, tactKeyProvider));
    }

    [Fact]
    public void UseMimironDb2_WithNullDbdProvider_ShouldThrow()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        var db2Provider = Substitute.For<IDb2StreamProvider>();
        var tactKeyProvider = Substitute.For<ITactKeyProvider>();

        Should.Throw<ArgumentNullException>(() => optionsBuilder.UseMimironDb2(db2Provider, null!, tactKeyProvider));
    }

    [Fact]
    public void UseMimironDb2_WithNullTactKeyProvider_ShouldThrow()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        var db2Provider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();

        Should.Throw<ArgumentNullException>(() => optionsBuilder.UseMimironDb2(db2Provider, dbdProvider, null!));
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
        var db2Provider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        var tactKeyProvider = Substitute.For<ITactKeyProvider>();

        var extension = new MimironDb2OptionsExtension().WithProviders(db2Provider, dbdProvider, tactKeyProvider);
        var options = new DbContextOptions<DbContext>();

        Should.NotThrow(() => extension.Validate(options));
    }

    [Fact]
    public void Info_ShouldReturnExtensionInfo()
    {
        var db2Provider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        var tactKeyProvider = Substitute.For<ITactKeyProvider>();

        var extension = new MimironDb2OptionsExtension().WithProviders(db2Provider, dbdProvider, tactKeyProvider);

        var info = extension.Info;

        info.ShouldNotBeNull();
        info.IsDatabaseProvider.ShouldBeTrue();
    }

    [Fact]
    public void Info_LogFragment_ShouldContainProviderType()
    {
        var db2Provider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        var tactKeyProvider = Substitute.For<ITactKeyProvider>();

        var extension = new MimironDb2OptionsExtension().WithProviders(db2Provider, dbdProvider, tactKeyProvider);

        var logFragment = extension.Info.LogFragment;

        logFragment.ShouldContain("MimironDb2");
        logFragment.ShouldContain("Db2=");
        logFragment.ShouldContain("Dbd=");
        logFragment.ShouldContain("TactKeys=");
    }

    [Fact]
    public void Info_LogFragment_WithoutPath_ShouldNotIncludePath()
    {
        var extension = new MimironDb2OptionsExtension();

        var logFragment = extension.Info.LogFragment;

        logFragment.ShouldContain("MimironDb2");
        logFragment.ShouldNotContain("Db2=");
        logFragment.ShouldNotContain("Dbd=");
        logFragment.ShouldNotContain("TactKeys=");
    }

    [Fact]
    public void Info_GetServiceProviderHashCode_ShouldBeConsistent()
    {
        var db2Provider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        var tactKeyProvider = Substitute.For<ITactKeyProvider>();

        var extension1 = new MimironDb2OptionsExtension().WithProviders(db2Provider, dbdProvider, tactKeyProvider);
        var extension2 = new MimironDb2OptionsExtension().WithProviders(db2Provider, dbdProvider, tactKeyProvider);

        var hash1 = extension1.Info.GetServiceProviderHashCode();
        var hash2 = extension2.Info.GetServiceProviderHashCode();

        hash1.ShouldBe(hash2);
    }

    [Fact]
    public void Info_ShouldUseSameServiceProvider_WithSameConfig_ShouldReturnTrue()
    {
        var db2Provider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        var tactKeyProvider = Substitute.For<ITactKeyProvider>();

        var extension1 = new MimironDb2OptionsExtension().WithProviders(db2Provider, dbdProvider, tactKeyProvider);
        var extension2 = new MimironDb2OptionsExtension().WithProviders(db2Provider, dbdProvider, tactKeyProvider);

        var result = extension1.Info.ShouldUseSameServiceProvider(extension2.Info);

        result.ShouldBeTrue();
    }

    [Fact]
    public void Info_ShouldUseSameServiceProvider_WithDifferentProviderInstances_ShouldReturnFalse()
    {
        var extension1 = new MimironDb2OptionsExtension().WithProviders(
            Substitute.For<IDb2StreamProvider>(),
            Substitute.For<IDbdProvider>(),
            Substitute.For<ITactKeyProvider>());

        var extension2 = new MimironDb2OptionsExtension().WithProviders(
            Substitute.For<IDb2StreamProvider>(),
            Substitute.For<IDbdProvider>(),
            Substitute.For<ITactKeyProvider>());

        var result = extension1.Info.ShouldUseSameServiceProvider(extension2.Info);

        result.ShouldBeFalse();
    }

    [Fact]
    public void Info_PopulateDebugInfo_ShouldAddProviderTypes()
    {
        var extension = new MimironDb2OptionsExtension().WithProviders(
            Substitute.For<IDb2StreamProvider>(),
            Substitute.For<IDbdProvider>(),
            Substitute.For<ITactKeyProvider>());
        var debugInfo = new Dictionary<string, string>();

        extension.Info.PopulateDebugInfo(debugInfo);

        debugInfo.ShouldContainKey("MimironDb2:Db2StreamProvider");
        debugInfo.ShouldContainKey("MimironDb2:DbdProvider");
        debugInfo.ShouldContainKey("MimironDb2:TactKeyProvider");
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

        var firstDb2 = Substitute.For<IDb2StreamProvider>();
        var firstDbd = Substitute.For<IDbdProvider>();
        var firstTact = Substitute.For<ITactKeyProvider>();

        var secondDb2 = Substitute.For<IDb2StreamProvider>();
        var secondDbd = Substitute.For<IDbdProvider>();
        var secondTact = Substitute.For<ITactKeyProvider>();

        optionsBuilder.UseMimironDb2(firstDb2, firstDbd, firstTact);
        optionsBuilder.UseMimironDb2(secondDb2, secondDbd, secondTact);

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();
        extension.Db2StreamProvider.ShouldBeSameAs(secondDb2);
        extension.DbdProvider.ShouldBeSameAs(secondDbd);
        extension.TactKeyProvider.ShouldBeSameAs(secondTact);
    }

    [Fact]
    public void UseMimironDb2_ShouldReturnOptionsBuilderForChaining()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        var db2Provider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        var tactKeyProvider = Substitute.For<ITactKeyProvider>();

        var result = optionsBuilder.UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider);

        result.ShouldBeSameAs(optionsBuilder);
    }
}
