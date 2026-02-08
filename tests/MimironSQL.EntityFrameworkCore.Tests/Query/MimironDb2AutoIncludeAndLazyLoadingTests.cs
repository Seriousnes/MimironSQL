using Microsoft.EntityFrameworkCore;

using MimironSQL.Providers;

using NSubstitute;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests.Query;

public sealed class MimironDb2AutoIncludeAndLazyLoadingTests
{
    [Fact]
    public void AutoInclude_eager_loads_reference_navigation_without_explicit_Include()
    {
        var testDataDir = TestDataPaths.GetIntegrationTestDataDirectory();

        var db2Provider = new FileSystemDb2StreamProvider(new FileSystemDb2StreamProviderOptions(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(testDataDir));
        var tactKeyProvider = Substitute.For<ITactKeyProvider>();
        tactKeyProvider.TryGetKey(Arg.Any<ulong>(), out Arg.Any<ReadOnlyMemory<byte>>()).Returns(false);

        var optionsBuilder = new DbContextOptionsBuilder<AutoIncludeContext>();
        optionsBuilder.UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider);

        using var context = new AutoIncludeContext(optionsBuilder.Options);

        var entity = context.MapChallengeModes
            .Where(x => x.MapID != 0)
            .First();

        entity.Map.ShouldNotBeNull();
        entity.Map!.Id.ShouldBe(entity.MapID);
    }

    [Fact]
    public void IgnoreAutoIncludes_disables_AutoInclude_loading()
    {
        var testDataDir = TestDataPaths.GetIntegrationTestDataDirectory();

        var db2Provider = new FileSystemDb2StreamProvider(new FileSystemDb2StreamProviderOptions(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(testDataDir));
        var tactKeyProvider = Substitute.For<ITactKeyProvider>();
        tactKeyProvider.TryGetKey(Arg.Any<ulong>(), out Arg.Any<ReadOnlyMemory<byte>>()).Returns(false);

        var optionsBuilder = new DbContextOptionsBuilder<AutoIncludeContext>();
        optionsBuilder.UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider);

        using var context = new AutoIncludeContext(optionsBuilder.Options);

        var entity = context.MapChallengeModes
            .IgnoreAutoIncludes()
            .Where(x => x.MapID != 0)
            .First();

        entity.Map.ShouldBeNull();
    }

    [Fact]
    public void Lazy_loading_proxies_load_reference_navigation_on_first_access()
    {
        var testDataDir = TestDataPaths.GetIntegrationTestDataDirectory();

        var db2Provider = new FileSystemDb2StreamProvider(new FileSystemDb2StreamProviderOptions(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(testDataDir));
        var tactKeyProvider = Substitute.For<ITactKeyProvider>();
        tactKeyProvider.TryGetKey(Arg.Any<ulong>(), out Arg.Any<ReadOnlyMemory<byte>>()).Returns(false);

        var optionsBuilder = new DbContextOptionsBuilder<LazyLoadingContext>();
        optionsBuilder.UseLazyLoadingProxies();
        optionsBuilder.UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider);

        using var context = new LazyLoadingContext(optionsBuilder.Options);

        var entity = context.MapChallengeModes
            .Where(x => x.MapID != 0)
            .First();

        entity.GetType().ShouldNotBe(typeof(MapChallengeMode));

        var related = entity.Map;
        related.ShouldNotBeNull();
        related!.Id.ShouldBe(entity.MapID);
    }

    private sealed class AutoIncludeContext(DbContextOptions<AutoIncludeContext> options) : DbContext(options)
    {
        public DbSet<Map> Maps => Set<Map>();
        public DbSet<MapChallengeMode> MapChallengeModes => Set<MapChallengeMode>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Map>().ToTable("Map");
            modelBuilder.Entity<MapChallengeMode>().ToTable("MapChallengeMode");

            modelBuilder.Entity<MapChallengeMode>()
                .HasOne(x => x.Map)
                .WithMany()
                .HasForeignKey(x => x.MapID);

            modelBuilder.Entity<MapChallengeMode>()
                .Navigation(x => x.Map)
                .AutoInclude();
        }
    }

    private sealed class LazyLoadingContext(DbContextOptions<LazyLoadingContext> options) : DbContext(options)
    {
        public DbSet<Map> Maps => Set<Map>();
        public DbSet<MapChallengeMode> MapChallengeModes => Set<MapChallengeMode>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Map>().ToTable("Map");
            modelBuilder.Entity<MapChallengeMode>().ToTable("MapChallengeMode");

            modelBuilder.Entity<MapChallengeMode>()
                .HasOne(x => x.Map)
                .WithMany()
                .HasForeignKey(x => x.MapID);
        }
    }

    public class Map
    {
        public int Id { get; set; }
    }

    public class MapChallengeMode
    {
        public int Id { get; set; }

        public int MapID { get; set; }

        public virtual Map? Map { get; set; }
    }

    private static class TestDataPaths
    {
        public static string GetIntegrationTestDataDirectory()
        {
            var baseDir = AppContext.BaseDirectory;
            return Path.GetFullPath(Path.Combine(
                baseDir,
                "..", "..", "..", "..",
                "MimironSQL.IntegrationTests",
                "TestData"));
        }
    }
}
