using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

using Shouldly;

using MimironSQL.Db2;
using MimironSQL.Dbd;
using MimironSQL.EntityFrameworkCore.Infrastructure;
using MimironSQL.Formats.Wdc5.Db2;
using MimironSQL.Providers;

namespace MimironSQL.EntityFrameworkCore.Tests.Db2.Query;

public sealed class CustomIndexQueryTests
{
    private const uint IndexedEntityLayoutHash = 0x12345678u;
    private const int IndexHeaderSize = 4096;
    private const int HeaderWowVersionOffset = 8;
    private const int HeaderLayoutHashOffset = 40;
    private const int HeaderWowVersionMaxBytes = 32;

    [Fact]
    public void WithCustomIndexes_SetsExtensionState()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        optionsBuilder.UseMimironDb2(o =>
        {
            o.WithWowVersion(TestHelpers.WowVersion);
            o.WithCustomIndexes(options => options.CacheDirectory = "C:\\temp\\mimironsql-indexes");
        });

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();

        extension.ShouldNotBeNull();
        extension!.EnableCustomIndexes.ShouldBeTrue();
        extension.CustomIndexCacheDirectory.ShouldBe("C:\\temp\\mimironsql-indexes");
    }

    [Fact]
    public void EqualityQuery_WithCustomIndexes_BuildsIndexAndReturnsMatches()
    {
        var cacheDirectory = Path.Combine(Path.GetTempPath(), $"mimironsql-index-tests-{Guid.NewGuid():N}");

        try
        {
            using var built = CreateContext<IndexedContext>(cacheDirectory);

            var results = built.Context.Entities
                .AsNoTracking()
                .Where(entity => entity.IndexValue == 20)
                .ToList();

            results.Count.ShouldBe(2);
            results.Select(static entity => entity.Id).OrderBy(static id => id).ShouldBe([101, 103]);
            results.All(static entity => entity.IndexValue == 20).ShouldBeTrue();

            var indexDirectory = Path.Combine(cacheDirectory, TestHelpers.WowVersion);
            Directory.Exists(indexDirectory).ShouldBeTrue();
            Directory.GetFiles(indexDirectory, "*.db2idx").Length.ShouldBe(1);
        }
        finally
        {
            TryDeleteDirectory(cacheDirectory);
        }
    }

    [Fact]
    public void EqualityQuery_WithStaleLayoutHashHeader_RebuildsIndex()
    {
        var cacheDirectory = Path.Combine(Path.GetTempPath(), $"mimironsql-index-tests-{Guid.NewGuid():N}");

        try
        {
            using (var built = CreateContext<IndexedContext>(cacheDirectory))
            {
                AssertMatchingRows(QueryByIndexValue(built.Context, 20));
            }

            var indexFilePath = GetSingleIndexFilePath(cacheDirectory);
            RewriteIndexHeader(indexFilePath, header => WriteUInt32(header, HeaderLayoutHashOffset, 0x87654321u));
            ReadIndexHeader(indexFilePath).LayoutHash.ShouldBe(0x87654321u);

            using var rebuilt = CreateContext<IndexedContext>(cacheDirectory);

            AssertMatchingRows(QueryByIndexValue(rebuilt.Context, 20));
            ReadIndexHeader(indexFilePath).LayoutHash.ShouldBe(IndexedEntityLayoutHash);
        }
        finally
        {
            TryDeleteDirectory(cacheDirectory);
        }
    }

    [Fact]
    public void EqualityQuery_WithStaleWowVersionHeader_RebuildsIndex()
    {
        var cacheDirectory = Path.Combine(Path.GetTempPath(), $"mimironsql-index-tests-{Guid.NewGuid():N}");

        try
        {
            using (var built = CreateContext<IndexedContext>(cacheDirectory))
            {
                AssertMatchingRows(QueryByIndexValue(built.Context, 20));
            }

            var indexFilePath = GetSingleIndexFilePath(cacheDirectory);
            RewriteIndexHeader(indexFilePath, header => WriteFixedLengthString(header, HeaderWowVersionOffset, HeaderWowVersionMaxBytes, "0.0.0.0"));
            ReadIndexHeader(indexFilePath).WowVersion.ShouldBe("0.0.0.0");

            using var rebuilt = CreateContext<IndexedContext>(cacheDirectory);

            AssertMatchingRows(QueryByIndexValue(rebuilt.Context, 20));
            ReadIndexHeader(indexFilePath).WowVersion.ShouldBe(TestHelpers.WowVersion);
        }
        finally
        {
            TryDeleteDirectory(cacheDirectory);
        }
    }

    [Fact]
    public void EqualityQuery_WithCorruptIndexFile_FallsBackToScan()
    {
        var cacheDirectory = Path.Combine(Path.GetTempPath(), $"mimironsql-index-tests-{Guid.NewGuid():N}");

        try
        {
            using var built = CreateContext<IndexedContext>(cacheDirectory);

            AssertMatchingRows(QueryByIndexValue(built.Context, 20));

            var indexFilePath = GetSingleIndexFilePath(cacheDirectory);
            File.WriteAllBytes(indexFilePath, [1, 2, 3]);

            AssertMatchingRows(QueryByIndexValue(built.Context, 20));
        }
        finally
        {
            TryDeleteDirectory(cacheDirectory);
        }
    }

    private static BuiltContext<TContext> CreateContext<TContext>(string cacheDirectory)
        where TContext : DbContext
    {
        var db2Bytes = CreateIndexedEntityDb2File(
        [
            (Id: 101, IndexValue: 20),
            (Id: 102, IndexValue: 30),
            (Id: 103, IndexValue: 20)
        ]);

        var optionsBuilder = new DbContextOptionsBuilder<TContext>();
        optionsBuilder.UseMimironDb2ForTests(o =>
        {
            o.WithCustomIndexes(options => options.CacheDirectory = cacheDirectory);
            o.ConfigureProvider(
                providerKey: "test-indexes",
                providerConfigHash: 0,
                applyProviderServices: services =>
                {
                    services.AddSingleton<IDbdProvider>(new IndexedEntityDbdProvider());
                    services.AddSingleton<IDb2StreamProvider>(new IndexedEntityStreamProvider(db2Bytes));
                });
        });

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();

        var services = new ServiceCollection();
        extension!.ApplyServices(services);

        var serviceProvider = services.BuildServiceProvider();
        optionsBuilder.UseInternalServiceProvider(serviceProvider);

        var context = (TContext)Activator.CreateInstance(typeof(TContext), optionsBuilder.Options)!;
        return new BuiltContext<TContext>(context, serviceProvider);
    }

    private static byte[] CreateIndexedEntityDb2File((int Id, int IndexValue)[] rows)
    {
        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        const int fieldsCount = 1;
        const int recordSize = 8;
        const int sectionsCount = 1;
        const int sectionFileOffset = 512;

        WriteWdc5Header(
            writer,
            recordsCount: rows.Length,
            fieldsCount: fieldsCount,
            recordSize: recordSize,
            stringTableSize: 0,
            minIndex: rows.Min(static row => row.Id),
            maxIndex: rows.Max(static row => row.Id),
            flags: 0,
            idFieldIndex: 0,
            sectionsCount: sectionsCount);

        WriteWdc5SectionHeader(writer, new Wdc5SectionHeader(
            TactKeyLookup: 0,
            FileOffset: sectionFileOffset,
            NumRecords: rows.Length,
            StringTableSize: 0,
            OffsetRecordsEndOffset: 0,
            IndexDataSize: rows.Length * 4,
            ParentLookupDataSize: 0,
            OffsetMapIDCount: 0,
            CopyTableCount: 0));

        writer.Write((short)1);
        writer.Write((short)0);

        var columnMeta = new ColumnMetaData
        {
            RecordOffset = 0,
            Size = 32,
            AdditionalDataSize = 0,
            CompressionType = CompressionType.Immediate,
            Immediate = new ColumnCompressionDataImmediate(BitOffset: 0, BitWidth: 32, Flags: 0),
        };
        WriteStruct(writer, columnMeta);

        PadTo(writer, sectionFileOffset);

        foreach (var row in rows)
        {
            writer.Write(row.IndexValue);
            writer.Write(0);
        }

        foreach (var row in rows)
        {
            writer.Write(row.Id);
        }

        return ms.ToArray();
    }

    private static List<IndexedEntity> QueryByIndexValue(IndexedContext context, int value)
        => context.Entities
            .AsNoTracking()
            .Where(entity => entity.IndexValue == value)
            .OrderBy(entity => entity.Id)
            .ToList();

    private static void AssertMatchingRows(IReadOnlyList<IndexedEntity> results)
    {
        results.Count.ShouldBe(2);
        results.Select(static entity => entity.Id).ShouldBe([101, 103]);
        results.All(static entity => entity.IndexValue == 20).ShouldBeTrue();
    }

    private static string GetSingleIndexFilePath(string cacheDirectory)
    {
        var indexDirectory = Path.Combine(cacheDirectory, TestHelpers.WowVersion);
        var files = Directory.GetFiles(indexDirectory, "*.db2idx");
        files.Length.ShouldBe(1);
        return files[0];
    }

    private static void RewriteIndexHeader(string filePath, Action<byte[]> mutate)
    {
        using var stream = new FileStream(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        var header = new byte[IndexHeaderSize];
        stream.ReadExactly(header);
        mutate(header);
        stream.Position = 0;
        stream.Write(header, 0, header.Length);
    }

    private static IndexHeader ReadIndexHeader(string filePath)
    {
        using var stream = File.OpenRead(filePath);
        var header = new byte[IndexHeaderSize];
        stream.ReadExactly(header);

        return new IndexHeader(
            ReadFixedLengthString(header, HeaderWowVersionOffset, HeaderWowVersionMaxBytes),
            ReadUInt32(header, HeaderLayoutHashOffset));
    }

    private static uint ReadUInt32(byte[] buffer, int offset)
        => BinaryPrimitives.ReadUInt32LittleEndian(buffer.AsSpan(offset));

    private static void WriteUInt32(byte[] buffer, int offset, uint value)
        => BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(offset), value);

    private static string ReadFixedLengthString(byte[] buffer, int offset, int maxBytes)
    {
        var span = buffer.AsSpan(offset, maxBytes);
        var end = span.IndexOf((byte)0);
        if (end < 0)
        {
            end = maxBytes;
        }

        return end == 0 ? string.Empty : Encoding.UTF8.GetString(span[..end]);
    }

    private static void WriteFixedLengthString(byte[] buffer, int offset, int maxBytes, string value)
    {
        var span = buffer.AsSpan(offset, maxBytes);
        span.Clear();
        var bytes = Encoding.UTF8.GetBytes(value);
        bytes.AsSpan(0, Math.Min(bytes.Length, maxBytes - 1)).CopyTo(span);
    }

    private static void WriteWdc5Header(
        BinaryWriter writer,
        int recordsCount,
        int fieldsCount,
        int recordSize,
        int stringTableSize,
        int minIndex,
        int maxIndex,
        Db2Flags flags,
        ushort idFieldIndex,
        int sectionsCount)
    {
        writer.Write(0x35434457u);
        writer.Write(1u);
        writer.Write(new byte[128]);
        writer.Write(recordsCount);
        writer.Write(fieldsCount);
        writer.Write(recordSize);
        writer.Write(stringTableSize);
        writer.Write(0u);
        writer.Write(IndexedEntityLayoutHash);
        writer.Write(minIndex);
        writer.Write(maxIndex);
        writer.Write(0);
        writer.Write((ushort)flags);
        writer.Write(idFieldIndex);
        writer.Write(0);
        writer.Write(0);
        writer.Write(0);
        writer.Write(0);
        writer.Write(0);
        writer.Write(0);
        writer.Write(sectionsCount);
    }

    private static void WriteWdc5SectionHeader(BinaryWriter writer, Wdc5SectionHeader header)
        => WriteStruct(writer, header);

    private static void WriteStruct<T>(BinaryWriter writer, T value) where T : unmanaged
    {
        Span<byte> bytes = stackalloc byte[Unsafe.SizeOf<T>()];
        MemoryMarshal.Write(bytes, in value);
        writer.Write(bytes);
    }

    private static void PadTo(BinaryWriter writer, int position)
    {
        while (writer.BaseStream.Position < position)
        {
            writer.Write((byte)0);
        }
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
        catch
        {
        }
    }

    private readonly record struct IndexHeader(string WowVersion, uint LayoutHash);

    private sealed record BuiltContext<TContext>(TContext Context, IServiceProvider ServiceProvider) : IDisposable
        where TContext : DbContext
    {
        public void Dispose()
        {
            Context.Dispose();
            (ServiceProvider as IDisposable)?.Dispose();
        }
    }

    private sealed class IndexedContext(DbContextOptions<IndexedContext> options) : DbContext(options)
    {
        public DbSet<IndexedEntity> Entities => Set<IndexedEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<IndexedEntity>(builder =>
            {
                builder.ToTable(nameof(IndexedEntity));
                builder.HasKey(entity => entity.Id);
                builder.Property(entity => entity.IndexValue);
            });
        }
    }

    private sealed class IndexedEntity
    {
        public int Id { get; set; }

        public int IndexValue { get; set; }
    }

    private sealed class IndexedEntityDbdProvider : IDbdProvider
    {
        public IDbdFile Open(string tableName)
        {
            if (!string.Equals(tableName, nameof(IndexedEntity), StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unexpected table name '{tableName}'.");
            }

            return new TestDbdFile(new TestDbdBuildBlock(
                TestHelpers.WowVersion,
                [
                    new TestDbdLayoutEntry("Id", Db2ValueType.Int64, isNonInline: true, isId: true),
                    new TestDbdLayoutEntry("IndexValue", Db2ValueType.Int64, isNonInline: false, isId: false)
                ]));
        }
    }

    private sealed class IndexedEntityStreamProvider(byte[] db2Bytes) : IDb2StreamProvider
    {
        public Stream OpenDb2Stream(string tableName)
        {
            if (!string.Equals(tableName, nameof(IndexedEntity), StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unexpected table name '{tableName}'.");
            }

            return new MemoryStream(db2Bytes, writable: false);
        }

        public Task<Stream> OpenDb2StreamAsync(string tableName, CancellationToken cancellationToken = default)
            => Task.FromResult(OpenDb2Stream(tableName));
    }

    private sealed class TestDbdFile(IDbdBuildBlock buildBlock) : IDbdFile
    {
        public IReadOnlyDictionary<string, IDbdColumn> ColumnsByName { get; } = new Dictionary<string, IDbdColumn>(StringComparer.Ordinal);
        public IReadOnlyList<IDbdLayout> Layouts { get; } = Array.Empty<IDbdLayout>();
        public IReadOnlyList<IDbdBuildBlock> GlobalBuilds { get; } = [buildBlock];

        public bool TryGetLayout(uint layoutHash, out IDbdLayout layout)
        {
            layout = default!;
            return false;
        }
    }

    private sealed class TestDbdBuildBlock(string buildLine, IReadOnlyList<IDbdLayoutEntry> entries) : IDbdBuildBlock
    {
        public string BuildLine { get; } = buildLine;
        public IReadOnlyList<IDbdLayoutEntry> Entries { get; } = entries;

        public int GetPhysicalColumnCount()
            => Entries.Count(static entry => !entry.IsNonInline);
    }

    private sealed class TestDbdLayoutEntry(string name, Db2ValueType valueType, bool isNonInline, bool isId) : IDbdLayoutEntry
    {
        public string Name { get; } = name;
        public Db2ValueType ValueType { get; } = valueType;
        public string? ReferencedTableName => null;
        public int ElementCount => 1;
        public bool IsVerified => true;
        public bool IsNonInline { get; } = isNonInline;
        public bool IsId { get; } = isId;
        public bool IsRelation => false;
        public string? InlineTypeToken => null;
    }
}
