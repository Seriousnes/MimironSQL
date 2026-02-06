using Microsoft.Extensions.DependencyInjection;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;
using MimironSQL.Providers;
using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public class MimironDb2ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddMimironDb2FileSystem_WithNullServices_ShouldThrow()
    {
        IServiceCollection services = null!;

        Should.Throw<ArgumentNullException>(() => services.AddMimironDb2FileSystem("/test/path"));
    }

    [Fact]
    public void AddMimironDb2FileSystem_WithNullPath_ShouldThrow()
    {
        var services = new ServiceCollection();

        Should.Throw<ArgumentException>(() => services.AddMimironDb2FileSystem(null!));
    }

    [Fact]
    public void AddMimironDb2FileSystem_WithEmptyPath_ShouldThrow()
    {
        var services = new ServiceCollection();

        Should.Throw<ArgumentException>(() => services.AddMimironDb2FileSystem(""));
    }

    [Fact]
    public void AddMimironDb2FileSystem_WithWhitespacePath_ShouldThrow()
    {
        var services = new ServiceCollection();

        Should.Throw<ArgumentException>(() => services.AddMimironDb2FileSystem("   "));
    }

    [Fact]
    public void AddMimironDb2FileSystem_ShouldRegisterRequiredServices()
    {
        var services = new ServiceCollection();
        var tempPath = Path.Combine(Path.GetTempPath(), "MimironSQLTest_" + Guid.NewGuid().ToString("N"));
        
        try
        {
            Directory.CreateDirectory(tempPath);
            services.AddMimironDb2FileSystem(tempPath);

            var serviceProvider = services.BuildServiceProvider();

            serviceProvider.GetService<IDb2StreamProvider>().ShouldNotBeNull();
            serviceProvider.GetService<IDbdProvider>().ShouldNotBeNull();
            serviceProvider.GetService<IDb2Format>().ShouldNotBeNull();
            serviceProvider.GetService<IMimironDb2Store>().ShouldNotBeNull();
        }
        finally
        {
            if (Directory.Exists(tempPath))
                Directory.Delete(tempPath, true);
        }
    }

    [Fact]
    public void AddMimironDb2FileSystem_WithDbdPath_ShouldUseCustomDbdPath()
    {
        var services = new ServiceCollection();

        services.AddMimironDb2FileSystem("/test/db2", "/test/dbd");

        var serviceProvider = services.BuildServiceProvider();

        var dbdProvider = serviceProvider.GetService<IDbdProvider>();
        dbdProvider.ShouldNotBeNull();
        dbdProvider.ShouldBeOfType<FileSystemDbdProvider>();
    }

    [Fact]
    public void AddMimironDb2FileSystem_WithoutDbdPath_ShouldUseDefaultDbdPath()
    {
        var services = new ServiceCollection();

        services.AddMimironDb2FileSystem("/test/path");

        var serviceProvider = services.BuildServiceProvider();

        var dbdProvider = serviceProvider.GetService<IDbdProvider>();
        dbdProvider.ShouldNotBeNull();
        dbdProvider.ShouldBeOfType<FileSystemDbdProvider>();
    }

    [Fact]
    public void AddMimironDb2FileSystem_ShouldReturnServiceCollection()
    {
        var services = new ServiceCollection();

        var result = services.AddMimironDb2FileSystem("/test/path");

        result.ShouldBe(services);
    }

    [Fact]
    public void AddMimironDb2FileSystem_ShouldRegisterServicesAsSingleton()
    {
        var services = new ServiceCollection();
        var tempPath = Path.Combine(Path.GetTempPath(), "MimironSQLTest_" + Guid.NewGuid().ToString("N"));
        
        try
        {
            Directory.CreateDirectory(tempPath);
            services.AddMimironDb2FileSystem(tempPath);

            var serviceProvider = services.BuildServiceProvider();

            var store1 = serviceProvider.GetService<IMimironDb2Store>();
            var store2 = serviceProvider.GetService<IMimironDb2Store>();

            store1.ShouldBe(store2);
        }
        finally
        {
            if (Directory.Exists(tempPath))
                Directory.Delete(tempPath, true);
        }
    }
}
