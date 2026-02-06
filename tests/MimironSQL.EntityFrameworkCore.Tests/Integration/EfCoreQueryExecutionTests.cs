using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests.Integration;

public class EfCoreQueryExecutionTests : IDisposable
{
    private readonly TestDbContext _context;

    public EfCoreQueryExecutionTests()
    {
        var testDataDir = GetTestDataDirectory();
        
        var services = new ServiceCollection();
        services.AddDbContext<TestDbContext>(options =>
            options.UseMimironDb2FileSystem(testDataDir));

        var serviceProvider = services.BuildServiceProvider();
        _context = serviceProvider.GetRequiredService<TestDbContext>();
    }

    private static string GetTestDataDirectory()
    {
        var assemblyDir = AppContext.BaseDirectory;
        var testDir = Path.GetFullPath(Path.Combine(assemblyDir, "../../../.."));
        return Path.Combine(testDir, "MimironSQL.IntegrationTests", "TestData");
    }

    [Fact]
    public void Can_query_with_where_clause()
    {
        var results = _context.Maps
            .Where(m => m.Id > 0)
            .Take(10)
            .ToList();

        results.ShouldNotBeEmpty();
        results.All(m => m.Id > 0).ShouldBeTrue();
    }

    [Fact]
    public void Can_query_with_select_projection()
    {
        var results = _context.Maps
            .Where(m => m.Id > 0)
            .Select(m => new { m.Id, m.Directory })
            .Take(10)
            .ToList();

        results.ShouldNotBeEmpty();
        results.All(r => r.Id > 0).ShouldBeTrue();
    }

    [Fact]
    public void Can_use_take_operator()
    {
        var results = _context.Maps
            .Take(5)
            .ToList();

        results.Count.ShouldBe(5);
    }

    [Fact]
    public void Can_use_skip_operator()
    {
        var allMaps = _context.Maps.Take(10).ToList();
        var skippedMaps = _context.Maps.Skip(5).Take(5).ToList();

        skippedMaps.Count.ShouldBe(5);
        skippedMaps.First().Id.ShouldBe(allMaps[5].Id);
    }

    [Fact]
    public void Can_use_count_operator()
    {
        var count = _context.Maps
            .Where(m => m.Id > 0)
            .Take(100)
            .Count();

        count.ShouldBeGreaterThan(0);
        count.ShouldBeLessThanOrEqualTo(100);
    }

    [Fact]
    public void Can_use_any_operator()
    {
        var hasAny = _context.Maps
            .Any(m => m.Id > 0);

        hasAny.ShouldBeTrue();
    }

    [Fact]
    public void Can_use_first_operator()
    {
        var first = _context.Maps
            .Where(m => m.Id > 0)
            .First();

        first.ShouldNotBeNull();
        first.Id.ShouldBeGreaterThan(0);
    }

    [Fact]
    public void Can_use_firstordefault_operator()
    {
        var first = _context.Maps
            .Where(m => m.Id > 0)
            .FirstOrDefault();

        first.ShouldNotBeNull();
    }

    [Fact]
    public void Can_use_single_operator_with_filter()
    {
        // Find a specific map by ID
        var map = _context.Maps.FirstOrDefault();
        map.ShouldNotBeNull();

        var single = _context.Maps
            .Where(m => m.Id == map!.Id)
            .Single();

        single.Id.ShouldBe(map.Id);
    }

    [Fact]
    public void Can_use_orderby_in_memory()
    {
        // OrderBy should work in-memory (not pushed down)
        var results = _context.Maps
            .Take(10)
            .AsEnumerable() // Force in-memory
            .OrderBy(m => m.Directory)
            .ToList();

        results.ShouldNotBeEmpty();
    }

    [Fact]
    public void Can_use_groupby_in_memory()
    {
        // GroupBy should work in-memory (not pushed down)
        var results = _context.Maps
            .Take(20)
            .AsEnumerable() // Force in-memory
            .GroupBy(m => m.Directory?.Length ?? 0)
            .ToList();

        results.ShouldNotBeEmpty();
    }

    public void Dispose()
    {
        _context?.Dispose();
    }
}

public class TestDbContext : DbContext
{
    public TestDbContext(DbContextOptions<TestDbContext> options) : base(options)
    {
    }

    public DbSet<Map> Maps => Set<Map>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Map>(entity =>
        {
            entity.ToTable("Map");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("ID");
            entity.Property(e => e.Directory).HasColumnName("Directory");
        });
    }
}

public class Map
{
    public int Id { get; set; }
    public string? Directory { get; set; }
}
