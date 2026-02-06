using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public class MimironDb2ModelCacheKeyFactoryTests
{
    [Fact]
    public void Extension_WithSameValues_ShouldProduceSameHash()
    {
        var extension1 = new MimironDb2OptionsExtension().WithFileSystem("/test/path", "/test/dbd");
        var extension2 = new MimironDb2OptionsExtension().WithFileSystem("/test/path", "/test/dbd");

        var hash1 = extension1.Info.GetServiceProviderHashCode();
        var hash2 = extension2.Info.GetServiceProviderHashCode();

        hash1.ShouldBe(hash2);
    }

    [Fact]
    public void Extension_WithDifferentPaths_ShouldProduceDifferentHash()
    {
        var extension1 = new MimironDb2OptionsExtension().WithFileSystem("/test/path1", "/test/dbd");
        var extension2 = new MimironDb2OptionsExtension().WithFileSystem("/test/path2", "/test/dbd");

        var hash1 = extension1.Info.GetServiceProviderHashCode();
        var hash2 = extension2.Info.GetServiceProviderHashCode();

        hash1.ShouldNotBe(hash2);
    }

    [Fact]
    public void Extension_WithDifferentDbdPaths_ShouldProduceDifferentHash()
    {
        var extension1 = new MimironDb2OptionsExtension().WithFileSystem("/test/path", "/test/dbd1");
        var extension2 = new MimironDb2OptionsExtension().WithFileSystem("/test/path", "/test/dbd2");

        var hash1 = extension1.Info.GetServiceProviderHashCode();
        var hash2 = extension2.Info.GetServiceProviderHashCode();

        hash1.ShouldNotBe(hash2);
    }

    [Fact]
    public void Extension_WithDifferentProviderTypes_ShouldProduceDifferentHash()
    {
        var extension1 = new MimironDb2OptionsExtension().WithFileSystem("/test/path", "/test/dbd");
        var extension2 = new MimironDb2OptionsExtension().WithCasc("/test/path", "/test/dbd");

        var hash1 = extension1.Info.GetServiceProviderHashCode();
        var hash2 = extension2.Info.GetServiceProviderHashCode();

        hash1.ShouldNotBe(hash2);
    }

    [Fact]
    public void Extension_ShouldUseSameServiceProvider_WithSameValues_ShouldReturnTrue()
    {
        var extension1 = new MimironDb2OptionsExtension().WithFileSystem("/test/path", "/test/dbd");
        var extension2 = new MimironDb2OptionsExtension().WithFileSystem("/test/path", "/test/dbd");

        var result = extension1.Info.ShouldUseSameServiceProvider(extension2.Info);

        result.ShouldBeTrue();
    }

    [Fact]
    public void Extension_ShouldUseSameServiceProvider_WithDifferentPaths_ShouldReturnFalse()
    {
        var extension1 = new MimironDb2OptionsExtension().WithFileSystem("/test/path1", "/test/dbd");
        var extension2 = new MimironDb2OptionsExtension().WithFileSystem("/test/path2", "/test/dbd");

        var result = extension1.Info.ShouldUseSameServiceProvider(extension2.Info);

        result.ShouldBeFalse();
    }
}
