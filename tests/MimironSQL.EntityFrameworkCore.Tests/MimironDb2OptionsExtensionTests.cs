using Microsoft.EntityFrameworkCore;
using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public class MimironDb2OptionsExtensionTests
{
    [Fact]
    public void UseMimironDb2FileSystem_ShouldConfigureOptionsExtension()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        optionsBuilder.UseMimironDb2FileSystem("/test/path");

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();
        extension.ProviderType.ShouldBe(MimironDb2ProviderType.FileSystem);
        extension.Db2Path.ShouldBe("/test/path");
    }

    [Fact]
    public void UseMimironDb2Casc_ShouldConfigureOptionsExtension()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        optionsBuilder.UseMimironDb2Casc("/test/casc");

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();
        extension.ProviderType.ShouldBe(MimironDb2ProviderType.Casc);
        extension.Db2Path.ShouldBe("/test/casc");
    }

    [Fact]
    public void Validate_WithoutPath_ShouldThrow()
    {
        var extension = new MimironDb2OptionsExtension();
        var options = new DbContextOptions<DbContext>();

        Should.Throw<InvalidOperationException>(() => extension.Validate(options))
            .Message.ShouldContain("DB2 path must be configured");
    }
}
