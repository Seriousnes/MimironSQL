using Microsoft.EntityFrameworkCore;

using NSubstitute;

using Shouldly;

using MimironSQL.Providers;

using System.Runtime.CompilerServices;
using MimironSQL.EntityFrameworkCore.Infrastructure;

namespace MimironSQL.EntityFrameworkCore.Tests;

public class MimironDb2ModelCacheKeyFactoryTests
{
    [Fact]
    public void Create_WithSameConfiguration_ShouldProduceSameKey()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();
        var extension = new MimironDb2OptionsExtension().WithProviders(
            Substitute.For<IDb2StreamProvider>(),
            Substitute.For<IDbdProvider>(),
            Substitute.For<ITactKeyProvider>());
        
        var key1 = CreateKeyFromExtension(factory, extension, typeof(TestContext), designTime: false);
        var key2 = CreateKeyFromExtension(factory, extension, typeof(TestContext), designTime: false);

        key1.ShouldBe(key2);
    }

    [Fact]
    public void Create_WithDifferentDb2Providers_ShouldProduceDifferentKeys()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();
        var sharedDbd = Substitute.For<IDbdProvider>();
        var sharedTact = Substitute.For<ITactKeyProvider>();

        var extension1 = new MimironDb2OptionsExtension().WithProviders(
            Substitute.For<IDb2StreamProvider>(),
            sharedDbd,
            sharedTact);

        var extension2 = new MimironDb2OptionsExtension().WithProviders(
            Substitute.For<IDb2StreamProvider>(),
            sharedDbd,
            sharedTact);
        
        var key1 = CreateKeyFromExtension(factory, extension1, typeof(TestContext), designTime: false);
        var key2 = CreateKeyFromExtension(factory, extension2, typeof(TestContext), designTime: false);

        key1.ShouldNotBe(key2);
    }

    [Fact]
    public void Create_WithDifferentDbdProviders_ShouldProduceDifferentKeys()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();
        var sharedDb2 = Substitute.For<IDb2StreamProvider>();
        var sharedTact = Substitute.For<ITactKeyProvider>();

        var extension1 = new MimironDb2OptionsExtension().WithProviders(
            sharedDb2,
            Substitute.For<IDbdProvider>(),
            sharedTact);

        var extension2 = new MimironDb2OptionsExtension().WithProviders(
            sharedDb2,
            Substitute.For<IDbdProvider>(),
            sharedTact);
        
        var key1 = CreateKeyFromExtension(factory, extension1, typeof(TestContext), designTime: false);
        var key2 = CreateKeyFromExtension(factory, extension2, typeof(TestContext), designTime: false);

        key1.ShouldNotBe(key2);
    }

    [Fact]
    public void Create_WithDifferentTactKeyProviders_ShouldProduceDifferentKeys()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();
        var sharedDb2 = Substitute.For<IDb2StreamProvider>();
        var sharedDbd = Substitute.For<IDbdProvider>();

        var extension1 = new MimironDb2OptionsExtension().WithProviders(
            sharedDb2,
            sharedDbd,
            Substitute.For<ITactKeyProvider>());

        var extension2 = new MimironDb2OptionsExtension().WithProviders(
            sharedDb2,
            sharedDbd,
            Substitute.For<ITactKeyProvider>());
        
        var key1 = CreateKeyFromExtension(factory, extension1, typeof(TestContext), designTime: false);
        var key2 = CreateKeyFromExtension(factory, extension2, typeof(TestContext), designTime: false);

        key1.ShouldNotBe(key2);
    }

    [Fact]
    public void Create_WithDifferentContextTypes_ShouldProduceDifferentKeys()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();
        var extension = new MimironDb2OptionsExtension().WithProviders(
            Substitute.For<IDb2StreamProvider>(),
            Substitute.For<IDbdProvider>(),
            Substitute.For<ITactKeyProvider>());
        
        var key1 = CreateKeyFromExtension(factory, extension, typeof(TestContext), designTime: false);
        var key2 = CreateKeyFromExtension(factory, extension, typeof(AnotherTestContext), designTime: false);

        key1.ShouldNotBe(key2);
    }

    [Fact]
    public void Create_WithDifferentDesignTimeFlag_ShouldProduceDifferentKeys()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();
        var extension = new MimironDb2OptionsExtension().WithProviders(
            Substitute.For<IDb2StreamProvider>(),
            Substitute.For<IDbdProvider>(),
            Substitute.For<ITactKeyProvider>());
        
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
            extension.Db2StreamProvider is null ? 0 : RuntimeHelpers.GetHashCode(extension.Db2StreamProvider),
            extension.DbdProvider is null ? 0 : RuntimeHelpers.GetHashCode(extension.DbdProvider),
            extension.TactKeyProvider is null ? 0 : RuntimeHelpers.GetHashCode(extension.TactKeyProvider),
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
