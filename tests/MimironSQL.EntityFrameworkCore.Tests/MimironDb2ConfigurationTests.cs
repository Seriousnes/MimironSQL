using Microsoft.EntityFrameworkCore;
using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public class MimironDb2ConfigurationTests
{
    [Fact]
    public void UseMimironDb2FileSystem_ConfiguresProvider()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        optionsBuilder.UseMimironDb2FileSystem("/test/db2/path");

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();
        extension.ProviderType.ShouldBe(MimironDb2ProviderType.FileSystem);
        extension.Db2Path.ShouldBe("/test/db2/path");
    }

    [Fact]
    public void UseMimironDb2FileSystem_WithDbdPath_ConfiguresProvider()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        optionsBuilder.UseMimironDb2FileSystem("/test/db2/path", "/test/dbd/path");

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();
        extension.ProviderType.ShouldBe(MimironDb2ProviderType.FileSystem);
        extension.Db2Path.ShouldBe("/test/db2/path");
        extension.DbdDefinitionsPath.ShouldBe("/test/dbd/path");
    }

    [Fact]
    public void UseMimironDb2Casc_ConfiguresProvider()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        optionsBuilder.UseMimironDb2Casc("/test/casc/path");

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();
        extension.ProviderType.ShouldBe(MimironDb2ProviderType.Casc);
        extension.Db2Path.ShouldBe("/test/casc/path");
    }

    [Fact]
    public void UseMimironDb2Casc_WithDbdPath_ConfiguresProvider()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        optionsBuilder.UseMimironDb2Casc("/test/casc/path", "/test/dbd/path");

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();
        extension.ProviderType.ShouldBe(MimironDb2ProviderType.Casc);
        extension.Db2Path.ShouldBe("/test/casc/path");
        extension.DbdDefinitionsPath.ShouldBe("/test/dbd/path");
    }

    [Fact]
    public void UseMimironDb2FileSystem_WithOptionsBuilder_InvokesCallback()
    {
        var optionsBuilder = new DbContextOptionsBuilder();
        var callbackInvoked = false;

        optionsBuilder.UseMimironDb2FileSystem("/test/path", configureOptions: builder =>
        {
            builder.ShouldNotBeNull();
            callbackInvoked = true;
        });

        callbackInvoked.ShouldBeTrue();
    }

    [Fact]
    public void UseMimironDb2Casc_WithOptionsBuilder_InvokesCallback()
    {
        var optionsBuilder = new DbContextOptionsBuilder();
        var callbackInvoked = false;

        optionsBuilder.UseMimironDb2Casc("/test/path", configureOptions: builder =>
        {
            builder.ShouldNotBeNull();
            callbackInvoked = true;
        });

        callbackInvoked.ShouldBeTrue();
    }

    [Fact]
    public void MultipleProviderConfigurations_LastOneWins()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        optionsBuilder.UseMimironDb2FileSystem("/filesystem/path");
        optionsBuilder.UseMimironDb2Casc("/casc/path");

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();
        extension.ProviderType.ShouldBe(MimironDb2ProviderType.Casc);
        extension.Db2Path.ShouldBe("/casc/path");
    }

    [Fact]
    public void CanChainFileSystemConfiguration()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        var result = optionsBuilder
            .UseMimironDb2FileSystem("/test/path", "/test/dbd");

        result.ShouldBeSameAs(optionsBuilder);
    }

    [Fact]
    public void CanChainCascConfiguration()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        var result = optionsBuilder
            .UseMimironDb2Casc("/test/path", "/test/dbd");

        result.ShouldBeSameAs(optionsBuilder);
    }
}
