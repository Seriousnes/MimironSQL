using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

using MimironSQL.EntityFrameworkCore;
using MimironSQL.EntityFrameworkCore.Infrastructure;
using MimironSQL.Providers;

using NSubstitute;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public class MimironDb2ModelCacheKeyFactoryTests
{
    [Fact]
    public void Create_WithSameConfiguration_ShouldProduceSameKey()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();
        var key1 = CreateKey<TestContext>(factory, providerKey: "Test", providerConfigHash: 1, designTime: false);
        var key2 = CreateKey<TestContext>(factory, providerKey: "Test", providerConfigHash: 1, designTime: false);

        key1.ShouldBe(key2);
    }

    [Fact]
    public void Create_WithDifferentProviderKeys_ShouldProduceDifferentKeys()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();
        var key1 = CreateKey<TestContext>(factory, providerKey: "ProviderA", providerConfigHash: 1, designTime: false);
        var key2 = CreateKey<TestContext>(factory, providerKey: "ProviderB", providerConfigHash: 1, designTime: false);

        key1.ShouldNotBe(key2);
    }

    [Fact]
    public void Create_WithDifferentProviderConfigHashes_ShouldProduceDifferentKeys()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();
        var key1 = CreateKey<TestContext>(factory, providerKey: "Test", providerConfigHash: 1, designTime: false);
        var key2 = CreateKey<TestContext>(factory, providerKey: "Test", providerConfigHash: 2, designTime: false);

        key1.ShouldNotBe(key2);
    }

    [Fact]
    public void Create_WithDifferentContextTypes_ShouldProduceDifferentKeys()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();
        var key1 = CreateKey<TestContext>(factory, providerKey: "Test", providerConfigHash: 1, designTime: false);
        var key2 = CreateKey<AnotherTestContext>(factory, providerKey: "Test", providerConfigHash: 1, designTime: false);

        key1.ShouldNotBe(key2);
    }

    [Fact]
    public void Create_WithDifferentDesignTimeFlag_ShouldProduceDifferentKeys()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();
        var key1 = CreateKey<TestContext>(factory, providerKey: "Test", providerConfigHash: 1, designTime: false);
        var key2 = CreateKey<TestContext>(factory, providerKey: "Test", providerConfigHash: 1, designTime: true);

        key1.ShouldNotBe(key2);
    }

    [Fact]
    public void Create_WithoutExtension_ShouldUseContextTypeAndDesignTimeOnly()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();

        var key = CreateKeyWithoutExtension(factory, typeof(TestContext), designTime: false);

        key.ShouldBe((typeof(TestContext), false));
    }

    [Fact]
    public void Create_WithNoMimironOptionsExtension_ShouldUseContextTypeAndDesignTimeOnly()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();

        using var ctx = new TestContext(
            new DbContextOptionsBuilder<TestContext>()
                .UseInMemoryDatabase(nameof(Create_WithNoMimironOptionsExtension_ShouldUseContextTypeAndDesignTimeOnly))
                .Options);
        factory.Create(ctx, designTime: true).ShouldBe((typeof(TestContext), true));
    }

    [Fact]
    public void Create_Overload_ShouldDefaultDesignTimeToFalse()
    {
        var factory = new MimironDb2ModelCacheKeyFactory();

        using var ctx = new TestContext(
            new DbContextOptionsBuilder<TestContext>()
                .UseInMemoryDatabase(nameof(Create_Overload_ShouldDefaultDesignTimeToFalse))
                .Options);
        factory.Create(ctx).ShouldBe((typeof(TestContext), false));
    }

    private static object CreateKey<TContext>(
        MimironDb2ModelCacheKeyFactory factory,
        string providerKey,
        int providerConfigHash,
        bool designTime)
        where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseMimironDb2(o => o.ConfigureProvider(
                providerKey: providerKey,
                providerConfigHash: providerConfigHash,
                applyProviderServices: services =>
                {
                    services.AddSingleton(Substitute.For<IDb2StreamProvider>());
                    services.AddSingleton(Substitute.For<IDbdProvider>());
                }))
            .Options;

        using var context = (TContext)Activator.CreateInstance(typeof(TContext), options)!;
        return factory.Create(context, designTime);
    }

    private static object CreateKeyWithoutExtension(
        MimironDb2ModelCacheKeyFactory factory,
        Type contextType,
        bool designTime)
    {
        return (contextType, designTime);
    }

    private sealed class TestContext(DbContextOptions options) : DbContext(options)
    {
    }

    private sealed class AnotherTestContext(DbContextOptions options) : DbContext(options)
    {
    }
}
