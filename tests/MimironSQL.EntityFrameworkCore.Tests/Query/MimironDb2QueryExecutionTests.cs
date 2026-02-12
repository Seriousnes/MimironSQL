using Microsoft.EntityFrameworkCore;

using MimironSQL.EntityFrameworkCore.Tests;
using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;
using MimironSQL.Providers;

using NSubstitute;

using Shouldly;

using Microsoft.Extensions.DependencyInjection;

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
        optionsBuilder.UseMimironDb2ForTests(o => o.UseFileSystem(
            db2DirectoryPath: testDataDir,
            dbdDefinitionsDirectory: Path.Combine(testDataDir, "definitions")));
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
        optionsBuilder.UseMimironDb2ForTests(o => o.UseFileSystem(
            db2DirectoryPath: testDataDir,
            dbdDefinitionsDirectory: Path.Combine(testDataDir, "definitions")));
        var options = optionsBuilder.Options;

        using var context = new GarrTypeContext(options);

        Should.Throw<NotSupportedException>(() => context.SaveChanges());
    }

    [Fact]
    public void KeyLookup_supports_multiple_ids_in_contains_in_id_list_order_and_opens_single_stream()
    {
        var testDataDir = TestDataPaths.GetIntegrationTestDataDirectory();

        var innerProvider = new FileSystemDb2StreamProvider(new FileSystemDb2StreamProviderOptions(testDataDir));
        var countingProvider = new CountingDb2StreamProvider(innerProvider);

        var ids = ReadFirstRowIds(innerProvider, tableName: "GarrType", count: 3);
        ids.Length.ShouldBe(3);

        var reordered = new[] { ids[2], ids[0], ids[1], ids[1] };

        var optionsBuilder = new DbContextOptionsBuilder<GarrTypeContext>();
        optionsBuilder.UseMimironDb2ForTests(o => o.ConfigureProvider(
            providerKey: "CountingFileSystem",
            providerConfigHash: 1,
            applyProviderServices: services =>
            {
                services.AddSingleton(new FileSystemDb2StreamProviderOptions(testDataDir));
                services.AddSingleton(new FileSystemDbdProviderOptions(Path.Combine(testDataDir, "definitions")));
                services.AddSingleton<IDb2StreamProvider>(countingProvider);
                services.AddSingleton<IDbdProvider, FileSystemDbdProvider>();
            }));

        using var context = new GarrTypeContext(optionsBuilder.Options);

        countingProvider.Reset();

        var results = context.GarrTypes
            .Where(x => reordered.Contains(x.Id))
            .ToList();

        results.Select(static x => x.Id).ToArray().ShouldBe(new[] { ids[0], ids[1], ids[2] });
        countingProvider.OpenCount.ShouldBe(1);

        countingProvider.Reset();

        var taken = context.GarrTypes
            .Where(x => reordered.Contains(x.Id))
            .Take(2)
            .ToList();

        taken.Select(static x => x.Id).ToArray().ShouldBe(new[] { ids[0], ids[1] });
        countingProvider.OpenCount.ShouldBe(1);
    }

    private static int ReadFirstRowId(IDb2StreamProvider db2Provider, string tableName)
    {
        using var stream = db2Provider.OpenDb2Stream(tableName);
        var file = (IDb2File<RowHandle>)new Wdc5Format().OpenFile(stream);
        return file.EnumerateRowHandles().First().RowId;
    }

    private static int[] ReadFirstRowIds(IDb2StreamProvider db2Provider, string tableName, int count)
    {
        using var stream = db2Provider.OpenDb2Stream(tableName);
        var file = (IDb2File<RowHandle>)new Wdc5Format().OpenFile(stream);
        return file.EnumerateRowHandles().Take(count).Select(static h => h.RowId).ToArray();
    }

    private sealed class CountingDb2StreamProvider(IDb2StreamProvider inner) : IDb2StreamProvider
    {
        private readonly IDb2StreamProvider _inner = inner;

        public int OpenCount { get; private set; }

        public void Reset() => OpenCount = 0;

        public Stream OpenDb2Stream(string tableName)
        {
            OpenCount++;
            return _inner.OpenDb2Stream(tableName);
        }

        public async Task<Stream> OpenDb2StreamAsync(string tableName, CancellationToken cancellationToken = default)
        {
            OpenCount++;
            return await _inner.OpenDb2StreamAsync(tableName, cancellationToken);
        }
    }

    private sealed class GarrTypeContext(DbContextOptions<GarrTypeContext> options) : DbContext(options)
    {
        public DbSet<GarrType> GarrTypes
        {
            get
            {
                return field ??= Set<GarrType>();
            }
        }

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
                "TestData"));
        }
    }
}
