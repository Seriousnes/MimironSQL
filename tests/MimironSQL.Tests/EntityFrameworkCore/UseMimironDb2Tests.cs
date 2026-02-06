using Microsoft.EntityFrameworkCore;
using Shouldly;

namespace MimironSQL.Tests.EntityFrameworkCore;

public sealed class UseMimironDb2Tests
{
    [Fact]
    public void UseMimironDb2_sets_no_tracking()
    {
        using var db = CreateContext();
        db.ChangeTracker.QueryTrackingBehavior.ShouldBe(QueryTrackingBehavior.NoTracking);
    }

    [Fact]
    public void SaveChanges_throws()
    {
        using var db = CreateContext();
        Should.Throw<NotSupportedException>(() => db.SaveChanges());
    }

    [Fact]
    public async Task SaveChangesAsync_throws()
    {
        await using var db = CreateContext();
        await Should.ThrowAsync<NotSupportedException>(() => db.SaveChangesAsync());
    }

    private static TestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString())
            .UseMimironDb2()
            .Options;

        return new TestDbContext(options);
    }

    private sealed class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options);
}
