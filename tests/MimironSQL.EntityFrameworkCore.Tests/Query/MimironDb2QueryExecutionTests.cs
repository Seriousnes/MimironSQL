using Microsoft.EntityFrameworkCore;

using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;
using MimironSQL.Providers;

using NSubstitute;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests.Query;

public sealed class MimironDb2QueryExecutionTests
{
    [Fact]
    public void DbSet_query_executes_against_real_db2_fixture()
    {
        var testDataDir = TestDataPaths.GetIntegrationTestDataDirectory();

        var db2Provider = new FileSystemDb2StreamProvider(new FileSystemDb2StreamProviderOptions(testDataDir));

        var expectedId = ReadFirstRowId(db2Provider, tableName: "GarrType");

        var optionsBuilder = new DbContextOptionsBuilder<GarrTypeContext>();
        optionsBuilder.UseMimironDb2(o => o.UseFileSystem(
            db2DirectoryPath: testDataDir,
            dbdDefinitionsDirectory: testDataDir));
        var options = optionsBuilder.Options;

        using var context = new GarrTypeContext(options);

        var actualId = context.GarrTypes
            .Where(x => x.Id == expectedId)
            .Select(x => x.Id)
            .Single();

        actualId.ShouldBe(expectedId);
    }

    [Fact]
    public void SaveChanges_throws_for_read_only_provider()
    {
        var testDataDir = TestDataPaths.GetIntegrationTestDataDirectory();

        var optionsBuilder = new DbContextOptionsBuilder<GarrTypeContext>();
        optionsBuilder.UseMimironDb2(o => o.UseFileSystem(
            db2DirectoryPath: testDataDir,
            dbdDefinitionsDirectory: testDataDir));
        var options = optionsBuilder.Options;

        using var context = new GarrTypeContext(options);

        Should.Throw<NotSupportedException>(() => context.SaveChanges());
    }

    private static int ReadFirstRowId(IDb2StreamProvider db2Provider, string tableName)
    {
        using var stream = db2Provider.OpenDb2Stream(tableName);
        var file = (IDb2File<RowHandle>)new Wdc5Format().OpenFile(stream);
        return file.EnumerateRowHandles().First().RowId;
    }

    private sealed class GarrTypeContext(DbContextOptions<GarrTypeContext> options) : DbContext(options)
    {
        public DbSet<GarrType> GarrTypes => Set<GarrType>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<GarrType>().ToTable("GarrType");
        }
    }

    private sealed class GarrType
    {
        public int Id { get; set; }
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
