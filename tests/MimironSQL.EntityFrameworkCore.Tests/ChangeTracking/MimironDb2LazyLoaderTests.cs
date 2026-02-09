using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

using MimironSQL.EntityFrameworkCore.ChangeTracking;
using MimironSQL.EntityFrameworkCore.Query;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Providers;

using NSubstitute;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class MimironDb2LazyLoaderTests
{
    [Fact]
    public async Task LoadAsync_WhenEntityNotInModel_CompletesWithoutThrowing()
    {
        var optionsBuilder = new DbContextOptionsBuilder<TestContext>();
        optionsBuilder.UseMimironDb2(o => o.ConfigureProvider(
            providerKey: "Test",
            providerConfigHash: 1,
            applyProviderServices: services =>
            {
                services.AddSingleton(Substitute.For<IDb2StreamProvider>());
                services.AddSingleton(Substitute.For<IDbdProvider>());
            }));
        var options = optionsBuilder.Options;

        await using var ctx = new TestContext(options);

        var current = Substitute.For<ICurrentDbContext>();
        current.Context.Returns(ctx);

        var store = Substitute.For<IMimironDb2Store>();
        var db2ModelProvider = Substitute.For<IMimironDb2Db2ModelProvider>();

        var loader = new MimironDb2LazyLoader(current, store, db2ModelProvider);

        await loader.LoadAsync(entity: new object(), cancellationToken: default, navigationName: "Nav");
    }

    private sealed class TestContext(DbContextOptions<TestContext> options) : DbContext(options);
}
