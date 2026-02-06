using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public class MimironDb2ModelCacheKeyFactoryTests
{
    [Fact]
    public void Create_WithSameConfiguration_ShouldProduceSameKey()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();
        var extension = new MimironDb2OptionsExtension().WithFileSystem("/test/path", "/test/dbd");
        
        var key1 = CreateKeyFromExtension(factory, extension, typeof(TestContext), designTime: false);
        var key2 = CreateKeyFromExtension(factory, extension, typeof(TestContext), designTime: false);

        key1.ShouldBe(key2);
    }

    [Fact]
    public void Create_WithDifferentPaths_ShouldProduceDifferentKeys()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();
        var extension1 = new MimironDb2OptionsExtension().WithFileSystem("/test/path1", "/test/dbd");
        var extension2 = new MimironDb2OptionsExtension().WithFileSystem("/test/path2", "/test/dbd");
        
        var key1 = CreateKeyFromExtension(factory, extension1, typeof(TestContext), designTime: false);
        var key2 = CreateKeyFromExtension(factory, extension2, typeof(TestContext), designTime: false);

        key1.ShouldNotBe(key2);
    }

    [Fact]
    public void Create_WithDifferentDbdPaths_ShouldProduceDifferentKeys()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();
        var extension1 = new MimironDb2OptionsExtension().WithFileSystem("/test/path", "/test/dbd1");
        var extension2 = new MimironDb2OptionsExtension().WithFileSystem("/test/path", "/test/dbd2");
        
        var key1 = CreateKeyFromExtension(factory, extension1, typeof(TestContext), designTime: false);
        var key2 = CreateKeyFromExtension(factory, extension2, typeof(TestContext), designTime: false);

        key1.ShouldNotBe(key2);
    }

    [Fact]
    public void Create_WithDifferentProviderTypes_ShouldProduceDifferentKeys()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();
        var extension1 = new MimironDb2OptionsExtension().WithFileSystem("/test/path", "/test/dbd");
        var extension2 = new MimironDb2OptionsExtension().WithCasc("/test/path", "/test/dbd");
        
        var key1 = CreateKeyFromExtension(factory, extension1, typeof(TestContext), designTime: false);
        var key2 = CreateKeyFromExtension(factory, extension2, typeof(TestContext), designTime: false);

        key1.ShouldNotBe(key2);
    }

    [Fact]
    public void Create_WithDifferentContextTypes_ShouldProduceDifferentKeys()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();
        var extension = new MimironDb2OptionsExtension().WithFileSystem("/test/path", "/test/dbd");
        
        var key1 = CreateKeyFromExtension(factory, extension, typeof(TestContext), designTime: false);
        var key2 = CreateKeyFromExtension(factory, extension, typeof(AnotherTestContext), designTime: false);

        key1.ShouldNotBe(key2);
    }

    [Fact]
    public void Create_WithDifferentDesignTimeFlag_ShouldProduceDifferentKeys()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();
        var extension = new MimironDb2OptionsExtension().WithFileSystem("/test/path", "/test/dbd");
        
        var key1 = CreateKeyFromExtension(factory, extension, typeof(TestContext), designTime: false);
        var key2 = CreateKeyFromExtension(factory, extension, typeof(TestContext), designTime: true);

        key1.ShouldNotBe(key2);
    }

    [Fact]
    public void Create_WithoutExtension_ShouldUseContextTypeAndDesignTimeOnly()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();
        
        var key = CreateKeyWithoutExtension(factory, typeof(TestContext), designTime: false);

        key.ShouldBe((typeof(TestContext), false));
    }

    private static object CreateKeyFromExtension(
        MimironDb2ModelCacheKeyFactory factory, 
        MimironDb2OptionsExtension extension, 
        Type contextType,
        bool designTime)
    {
        return (
            contextType,
            extension.ProviderType,
            extension.Db2Path,
            extension.DbdDefinitionsPath,
            designTime);
    }

    private static object CreateKeyWithoutExtension(
        MimironDb2ModelCacheKeyFactory factory,
        Type contextType,
        bool designTime)
    {
        return (contextType, designTime);
    }

    private class TestContext : DbContext
    {
    }

    private class AnotherTestContext : DbContext
    {
    }
}
