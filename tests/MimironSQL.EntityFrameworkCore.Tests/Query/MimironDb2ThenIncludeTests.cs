using Microsoft.EntityFrameworkCore;

using MimironSQL.Providers;

using NSubstitute;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests.Query;

public sealed class MimironDb2ThenIncludeTests
{
    [Fact]
    public void ThenInclude_collection_then_reference_loads_nested_navigation()
    {
        var testDataDir = TestDataPaths.GetIntegrationTestDataDirectory();

        var db2Provider = new FileSystemDb2StreamProvider(new FileSystemDb2StreamProviderOptions(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(testDataDir));
        var tactKeyProvider = Substitute.For<ITactKeyProvider>();
        tactKeyProvider.TryGetKey(Arg.Any<ulong>(), out Arg.Any<ReadOnlyMemory<byte>>()).Returns(false);

        var optionsBuilder = new DbContextOptionsBuilder<ThenIncludeContext>();
        optionsBuilder.UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider);

        using var context = new ThenIncludeContext(optionsBuilder.Options);

        var mapId = context.MapChallengeModes
            .Where(x => x.MapID != 0)
            .Select(x => x.MapID)
            .First();

        var map = context.Maps
            .Where(x => x.Id == mapId)
            .Include(x => x.MapChallengeModes)
            .ThenInclude(x => x.Map)
            .Single();

        map.MapChallengeModes.Count.ShouldBeGreaterThan(0);
        foreach (var mode in map.MapChallengeModes)
        {
            var modeMap = mode.Map ?? throw new InvalidOperationException("Expected ThenInclude to populate MapChallengeMode.Map.");
            modeMap.Id.ShouldBe(map.Id);
        }
    }

    [Fact]
    public void ThenInclude_reference_then_collection_loads_nested_navigation()
    {
        var testDataDir = TestDataPaths.GetIntegrationTestDataDirectory();

        var db2Provider = new FileSystemDb2StreamProvider(new FileSystemDb2StreamProviderOptions(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(testDataDir));
        var tactKeyProvider = Substitute.For<ITactKeyProvider>();
        tactKeyProvider.TryGetKey(Arg.Any<ulong>(), out Arg.Any<ReadOnlyMemory<byte>>()).Returns(false);

        var optionsBuilder = new DbContextOptionsBuilder<ThenIncludeContext>();
        optionsBuilder.UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider);

        using var context = new ThenIncludeContext(optionsBuilder.Options);

        var entity = context.MapChallengeModes
            .Where(x => x.MapID != 0)
            .Include(x => x.Map)
            .ThenInclude(x => x.MapChallengeModes)
            .First();

        var map = entity.Map ?? throw new InvalidOperationException("Expected Include to populate MapChallengeMode.Map.");
        map.MapChallengeModes.Count.ShouldBeGreaterThan(0);
        map.MapChallengeModes.Any(x => x.Id == entity.Id).ShouldBeTrue();
    }

    private sealed class ThenIncludeContext(DbContextOptions<ThenIncludeContext> options) : DbContext(options)
    {
        public DbSet<Map> Maps => Set<Map>();
        public DbSet<MapChallengeMode> MapChallengeModes => Set<MapChallengeMode>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Map>().ToTable("Map");
            modelBuilder.Entity<MapChallengeMode>().ToTable("MapChallengeMode");

            modelBuilder.Entity<MapChallengeMode>()
                .HasOne(x => x.Map)
                .WithMany(x => x.MapChallengeModes)
                .HasForeignKey(x => x.MapID)
                .IsRequired();
        }
    }

    public class Map
    {
        public int Id { get; set; }

        public ICollection<MapChallengeMode> MapChallengeModes { get; set; } = [];
    }

    public class MapChallengeMode
    {
        public int Id { get; set; }

        public int MapID { get; set; }

        public Map Map { get; set; } = null!;
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
