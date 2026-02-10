using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

using MimironSQL.EntityFrameworkCore.Diagnostics;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class ServiceRegistrationTests
{
    [Fact]
    public void AddMimironSQLServices_registers_core_services_and_is_chainable()
    {
        var services = new ServiceCollection();

        services.AddMimironSQLServices().ShouldBeSameAs(services);

        services.Any(s => s.ServiceType == typeof(IMimironDb2Store)).ShouldBeTrue();

        using var provider = services.BuildServiceProvider();

        provider.GetService<IDb2Format>().ShouldNotBeNull();

        var interceptors = provider.GetServices<IInterceptor>().ToArray();
        interceptors.Any(x => x is MimironDb2ReadOnlySaveChangesInterceptor).ShouldBeTrue();
    }

    [Fact]
    public async Task Read_only_SaveChanges_interceptor_throws_for_sync_and_async()
    {
        var interceptor = new MimironDb2ReadOnlySaveChangesInterceptor();

        var ex1 = Should.Throw<NotSupportedException>(() => interceptor.SavingChanges(null!, default));
        ex1.Message.ShouldContain("read-only");
        ex1.Message.ShouldContain("SaveChanges");

        var ex2 = await Should.ThrowAsync<NotSupportedException>(async () =>
            await interceptor.SavingChangesAsync(null!, default, CancellationToken.None));

        ex2.Message.ShouldContain("read-only");
        ex2.Message.ShouldContain("SaveChangesAsync");
    }
}
