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
    public void UseMimironDb2FileSystem_WithDbdPath_ShouldConfigureExtension()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        optionsBuilder.UseMimironDb2FileSystem("/test/path", "/test/dbd");

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();
        extension.DbdDefinitionsPath.ShouldBe("/test/dbd");
    }

    [Fact]
    public void UseMimironDb2FileSystem_WithConfigureOptions_ShouldInvokeCallback()
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
    public void UseMimironDb2FileSystem_WithNullOptionsBuilder_ShouldThrow()
    {
        DbContextOptionsBuilder optionsBuilder = null!;

        Should.Throw<ArgumentNullException>(() => optionsBuilder.UseMimironDb2FileSystem("/test/path"));
    }

    [Fact]
    public void UseMimironDb2FileSystem_WithNullPath_ShouldThrow()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        Should.Throw<ArgumentException>(() => optionsBuilder.UseMimironDb2FileSystem(null!));
    }

    [Fact]
    public void UseMimironDb2FileSystem_WithEmptyPath_ShouldThrow()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        Should.Throw<ArgumentException>(() => optionsBuilder.UseMimironDb2FileSystem(""));
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
    public void UseMimironDb2Casc_WithDbdPath_ShouldConfigureExtension()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        optionsBuilder.UseMimironDb2Casc("/test/casc", "/test/dbd");

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();
        extension.DbdDefinitionsPath.ShouldBe("/test/dbd");
    }

    [Fact]
    public void UseMimironDb2Casc_WithConfigureOptions_ShouldInvokeCallback()
    {
        var optionsBuilder = new DbContextOptionsBuilder();
        var callbackInvoked = false;

        optionsBuilder.UseMimironDb2Casc("/test/casc", configureOptions: builder =>
        {
            builder.ShouldNotBeNull();
            callbackInvoked = true;
        });

        callbackInvoked.ShouldBeTrue();
    }

    [Fact]
    public void UseMimironDb2Casc_WithNullOptionsBuilder_ShouldThrow()
    {
        DbContextOptionsBuilder optionsBuilder = null!;

        Should.Throw<ArgumentNullException>(() => optionsBuilder.UseMimironDb2Casc("/test/casc"));
    }

    [Fact]
    public void UseMimironDb2Casc_WithNullPath_ShouldThrow()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        Should.Throw<ArgumentException>(() => optionsBuilder.UseMimironDb2Casc(null!));
    }

    [Fact]
    public void UseMimironDb2Casc_WithEmptyPath_ShouldThrow()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        Should.Throw<ArgumentException>(() => optionsBuilder.UseMimironDb2Casc(""));
    }

    [Fact]
    public void Validate_WithoutPath_ShouldThrow()
    {
        var extension = new MimironDb2OptionsExtension();
        var options = new DbContextOptions<DbContext>();

        Should.Throw<InvalidOperationException>(() => extension.Validate(options))
            .Message.ShouldContain("DB2 path must be configured");
    }

    [Fact]
    public void Validate_WithPath_ShouldNotThrow()
    {
        var extension = new MimironDb2OptionsExtension().WithFileSystem("/test/path", null);
        var options = new DbContextOptions<DbContext>();

        Should.NotThrow(() => extension.Validate(options));
    }

    [Fact]
    public void Info_ShouldReturnExtensionInfo()
    {
        var extension = new MimironDb2OptionsExtension().WithFileSystem("/test/path", null);

        var info = extension.Info;

        info.ShouldNotBeNull();
        info.IsDatabaseProvider.ShouldBeTrue();
    }

    [Fact]
    public void Info_LogFragment_ShouldContainProviderType()
    {
        var extension = new MimironDb2OptionsExtension().WithFileSystem("/test/path", null);

        var logFragment = extension.Info.LogFragment;

        logFragment.ShouldContain("MimironDb2:");
        logFragment.ShouldContain("FileSystem");
        logFragment.ShouldContain("/test/path");
    }

    [Fact]
    public void Info_LogFragment_WithoutPath_ShouldNotIncludePath()
    {
        var extension = new MimironDb2OptionsExtension();

        var logFragment = extension.Info.LogFragment;

        logFragment.ShouldContain("MimironDb2:");
        logFragment.ShouldNotContain("=");
    }

    [Fact]
    public void Info_GetServiceProviderHashCode_ShouldBeConsistent()
    {
        var extension1 = new MimironDb2OptionsExtension().WithFileSystem("/test/path", "/test/dbd");
        var extension2 = new MimironDb2OptionsExtension().WithFileSystem("/test/path", "/test/dbd");

        var hash1 = extension1.Info.GetServiceProviderHashCode();
        var hash2 = extension2.Info.GetServiceProviderHashCode();

        hash1.ShouldBe(hash2);
    }

    [Fact]
    public void Info_ShouldUseSameServiceProvider_WithSameConfig_ShouldReturnTrue()
    {
        var extension1 = new MimironDb2OptionsExtension().WithFileSystem("/test/path", "/test/dbd");
        var extension2 = new MimironDb2OptionsExtension().WithFileSystem("/test/path", "/test/dbd");

        var result = extension1.Info.ShouldUseSameServiceProvider(extension2.Info);

        result.ShouldBeTrue();
    }

    [Fact]
    public void Info_ShouldUseSameServiceProvider_WithDifferentPath_ShouldReturnFalse()
    {
        var extension1 = new MimironDb2OptionsExtension().WithFileSystem("/test/path1", null);
        var extension2 = new MimironDb2OptionsExtension().WithFileSystem("/test/path2", null);

        var result = extension1.Info.ShouldUseSameServiceProvider(extension2.Info);

        result.ShouldBeFalse();
    }

    [Fact]
    public void Info_ShouldUseSameServiceProvider_WithDifferentProviderType_ShouldReturnFalse()
    {
        var extension1 = new MimironDb2OptionsExtension().WithFileSystem("/test/path", null);
        var extension2 = new MimironDb2OptionsExtension().WithCasc("/test/path", null);

        var result = extension1.Info.ShouldUseSameServiceProvider(extension2.Info);

        result.ShouldBeFalse();
    }

    [Fact]
    public void Info_PopulateDebugInfo_ShouldAddProviderType()
    {
        var extension = new MimironDb2OptionsExtension().WithFileSystem("/test/path", null);
        var debugInfo = new Dictionary<string, string>();

        extension.Info.PopulateDebugInfo(debugInfo);

        debugInfo.ShouldContainKey("MimironDb2:ProviderType");
        debugInfo["MimironDb2:ProviderType"].ShouldBe("FileSystem");
    }

    [Fact]
    public void Info_PopulateDebugInfo_WithPath_ShouldAddPath()
    {
        var extension = new MimironDb2OptionsExtension().WithFileSystem("/test/path", null);
        var debugInfo = new Dictionary<string, string>();

        extension.Info.PopulateDebugInfo(debugInfo);

        debugInfo.ShouldContainKey("MimironDb2:Path");
        debugInfo["MimironDb2:Path"].ShouldBe("/test/path");
    }

    [Fact]
    public void Info_PopulateDebugInfo_WithDbdPath_ShouldAddDbdPath()
    {
        var extension = new MimironDb2OptionsExtension().WithFileSystem("/test/path", "/test/dbd");
        var debugInfo = new Dictionary<string, string>();

        extension.Info.PopulateDebugInfo(debugInfo);

        debugInfo.ShouldContainKey("MimironDb2:DbdDefinitionsPath");
        debugInfo["MimironDb2:DbdDefinitionsPath"].ShouldBe("/test/dbd");
    }

    [Fact]
    public void ApplyServices_ShouldNotThrow()
    {
        var extension = new MimironDb2OptionsExtension();
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();

        Should.NotThrow(() => extension.ApplyServices(services));
    }

    [Fact]
    public void ApplyServices_WithCascProvider_ShouldThrow()
    {
        var extension = new MimironDb2OptionsExtension().WithCasc("/test/casc", null);
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();

        var exception = Should.Throw<NotSupportedException>(() => extension.ApplyServices(services));
        exception.Message.ShouldContain("CASC provider is not yet supported");
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

        optionsBuilder.UseMimironDb2FileSystem("/filesystem/path");
        optionsBuilder.UseMimironDb2Casc("/casc/path");

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();
        extension.ProviderType.ShouldBe(MimironDb2ProviderType.Casc);
        extension.Db2Path.ShouldBe("/casc/path");
    }

    [Fact]
    public void UseMimironDb2FileSystem_ShouldReturnOptionsBuilderForChaining()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        var result = optionsBuilder.UseMimironDb2FileSystem("/test/path");

        result.ShouldBeSameAs(optionsBuilder);
    }

    [Fact]
    public void UseMimironDb2Casc_ShouldReturnOptionsBuilderForChaining()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        var result = optionsBuilder.UseMimironDb2Casc("/test/path");

        result.ShouldBeSameAs(optionsBuilder);
    }

    [Fact]
    public void UseMimironDb2FileSystem_Generic_ShouldReturnSameBuilderForChaining()
    {
        var optionsBuilder = new DbContextOptionsBuilder<DbContext>();

        var result = optionsBuilder.UseMimironDb2FileSystem("/test/path");

        result.ShouldBeSameAs(optionsBuilder);
    }

    [Fact]
    public void UseMimironDb2FileSystem_Generic_ShouldConfigureOptionsExtension()
    {
        var optionsBuilder = new DbContextOptionsBuilder<DbContext>();

        optionsBuilder.UseMimironDb2FileSystem("/test/path");

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();
        extension.ProviderType.ShouldBe(MimironDb2ProviderType.FileSystem);
        extension.Db2Path.ShouldBe("/test/path");
    }

    [Fact]
    public void UseMimironDb2FileSystem_Generic_WithDbdPath_ShouldConfigureExtension()
    {
        var optionsBuilder = new DbContextOptionsBuilder<DbContext>();

        optionsBuilder.UseMimironDb2FileSystem("/test/path", "/test/dbd");

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();
        extension.ProviderType.ShouldBe(MimironDb2ProviderType.FileSystem);
        extension.Db2Path.ShouldBe("/test/path");
        extension.DbdDefinitionsPath.ShouldBe("/test/dbd");
    }

    [Fact]
    public void UseMimironDb2FileSystem_Generic_WithConfigureOptions_ShouldInvokeCallback()
    {
        var optionsBuilder = new DbContextOptionsBuilder<DbContext>();
        var callbackInvoked = false;

        optionsBuilder.UseMimironDb2FileSystem("/test/path", configureOptions: builder =>
        {
            builder.ShouldNotBeNull();
            callbackInvoked = true;
        });

        callbackInvoked.ShouldBeTrue();
    }

    [Fact]
    public void UseMimironDb2Casc_Generic_ShouldReturnSameBuilderForChaining()
    {
        var optionsBuilder = new DbContextOptionsBuilder<DbContext>();

        var result = optionsBuilder.UseMimironDb2Casc("/test/casc");

        result.ShouldBeSameAs(optionsBuilder);
    }

    [Fact]
    public void UseMimironDb2Casc_Generic_ShouldConfigureOptionsExtension()
    {
        var optionsBuilder = new DbContextOptionsBuilder<DbContext>();

        optionsBuilder.UseMimironDb2Casc("/test/casc");

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();
        extension.ProviderType.ShouldBe(MimironDb2ProviderType.Casc);
        extension.Db2Path.ShouldBe("/test/casc");
    }

    [Fact]
    public void UseMimironDb2Casc_Generic_WithDbdPath_ShouldConfigureExtension()
    {
        var optionsBuilder = new DbContextOptionsBuilder<DbContext>();

        optionsBuilder.UseMimironDb2Casc("/test/casc", "/test/dbd");

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();
        extension.ProviderType.ShouldBe(MimironDb2ProviderType.Casc);
        extension.Db2Path.ShouldBe("/test/casc");
        extension.DbdDefinitionsPath.ShouldBe("/test/dbd");
    }

    [Fact]
    public void UseMimironDb2Casc_Generic_WithConfigureOptions_ShouldInvokeCallback()
    {
        var optionsBuilder = new DbContextOptionsBuilder<DbContext>();
        var callbackInvoked = false;

        optionsBuilder.UseMimironDb2Casc("/test/casc", configureOptions: builder =>
        {
            builder.ShouldNotBeNull();
            callbackInvoked = true;
        });

        callbackInvoked.ShouldBeTrue();
    }
}
