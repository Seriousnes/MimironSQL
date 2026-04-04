using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

using MimironSQL.Db2;
using MimironSQL.Dbd;
using MimironSQL.EntityFrameworkCore.Infrastructure;
using MimironSQL.EntityFrameworkCore.Model;
using MimironSQL.EntityFrameworkCore.Query.Internal;
using MimironSQL.Formats;
using MimironSQL.Providers;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests.Db2.Query;

public sealed class IncludeFkGroupingCacheTests
{
    [Fact]
    public void Collection_include_reuses_fk_grouping_cache_across_DbContext_instances()
    {
        var dbdProvider = new TestDbdProvider(new Dictionary<string, IDbdFile>(StringComparer.Ordinal)
        {
            [nameof(MapEntity)] = TestDbdFile.Create(
                buildLine: TestHelpers.WowVersion,
                entries:
                [
                    new TestDbdLayoutEntry("Id", Db2ValueType.Int64, isNonInline: true, isId: true),
                ]),
            [nameof(MapChallengeModeEntity)] = TestDbdFile.Create(
                buildLine: TestHelpers.WowVersion,
                entries:
                [
                    new TestDbdLayoutEntry("Id", Db2ValueType.Int64, isNonInline: true, isId: true),
                    new TestDbdLayoutEntry("MapId", Db2ValueType.Int64),
                ]),
        });

        var mapsFile = new CountingDb2File(
            header: new CountingDb2FileHeader(layoutHash: 0, fieldsCount: 0),
            rowHandles:
            [
                new RowHandle(0, 0, rowId: 1),
                new RowHandle(0, 1, rowId: 2),
                new RowHandle(0, 2, rowId: 3),
            ],
            physicalFields: new Dictionary<(int RowId, int FieldIndex), object?>());

        var modesFile = new CountingDb2File(
            header: new CountingDb2FileHeader(layoutHash: 0, fieldsCount: 1),
            rowHandles:
            [
                new RowHandle(0, 0, rowId: 101),
                new RowHandle(0, 1, rowId: 102),
                new RowHandle(0, 2, rowId: 103),
                new RowHandle(0, 3, rowId: 104),
                new RowHandle(0, 4, rowId: 105),
            ],
            physicalFields: new Dictionary<(int RowId, int FieldIndex), object?>
            {
                [(101, 0)] = 1,
                [(102, 0)] = 1,
                [(103, 0)] = 2,
                [(104, 0)] = 2,
                [(105, 0)] = 999,
            });

        var streamProvider = new TestDb2StreamProvider();
        var format = new TestDb2Format(new Dictionary<string, IDb2File>(StringComparer.Ordinal)
        {
            [nameof(MapEntity)] = mapsFile,
            [nameof(MapChallengeModeEntity)] = modesFile,
        });

        var optionsBuilder = new DbContextOptionsBuilder<IncludeContext>();
        optionsBuilder.UseMimironDb2ForTests(o =>
        {
            o.ConfigureProvider(
                providerKey: "test",
                providerConfigHash: 0,
                applyProviderServices: services =>
                {
                    services.AddSingleton<IDbdProvider>(dbdProvider);
                    services.AddSingleton<IDb2StreamProvider>(streamProvider);
                    services.AddSingleton<IDb2Format>(format);
                });
        });

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();

        var services = new ServiceCollection();
        extension!.ApplyServices(services);

        using var serviceProvider = services.BuildServiceProvider();
        optionsBuilder.UseInternalServiceProvider(serviceProvider);

        var options = optionsBuilder.Options;

        using (var context1 = new IncludeContext(options))
        {
            var maps = context1.Maps
                .AsNoTracking()
                .Include(x => x.MapChallengeModes)
                .ToList();

            maps.Count.ShouldBe(3);
            maps.Single(x => x.Id == 1).MapChallengeModes.Select(x => x.Id).OrderBy(static x => x).ToArray().ShouldBe([101, 102]);
            maps.Single(x => x.Id == 2).MapChallengeModes.Select(x => x.Id).OrderBy(static x => x).ToArray().ShouldBe([103, 104]);
            maps.Single(x => x.Id == 3).MapChallengeModes.ShouldBeEmpty();

            modesFile.EnumerateRowHandlesCalls.ShouldBe(1);
        }

        var tryGetRowCallsAfterFirst = modesFile.TryGetRowByIdCalls;
        tryGetRowCallsAfterFirst.ShouldBeGreaterThan(0);

        using (var context2 = new IncludeContext(options))
        {
            var maps = context2.Maps
                .AsNoTracking()
                .Include(x => x.MapChallengeModes)
                .ToList();

            maps.Count.ShouldBe(3);
            maps.Single(x => x.Id == 1).MapChallengeModes.Select(x => x.Id).OrderBy(static x => x).ToArray().ShouldBe([101, 102]);
            maps.Single(x => x.Id == 2).MapChallengeModes.Select(x => x.Id).OrderBy(static x => x).ToArray().ShouldBe([103, 104]);
            maps.Single(x => x.Id == 3).MapChallengeModes.ShouldBeEmpty();

            // The FK grouping should be reused, so we should not enumerate the dependent table again.
            modesFile.EnumerateRowHandlesCalls.ShouldBe(1);
            modesFile.TryGetRowByIdCalls.ShouldBeGreaterThan(tryGetRowCallsAfterFirst);
        }

        using (var context3 = new IncludeContext(options))
        {
            var cache1 = context3.GetService<Db2FkGroupingCache>();
            using var context4 = new IncludeContext(options);
            var cache2 = context4.GetService<Db2FkGroupingCache>();

            cache1.ShouldBeSameAs(cache2);
        }
    }

    [Fact]
    public void Db2FkGroupingCache_GetOrBuild_is_thread_safe_and_runs_factory_once()
    {
        var cache = new Db2FkGroupingCache();
        var key = new Db2FkGroupingCache.Key("Foo", "*", ForeignKeyFieldIndex: 0);

        var buildCount = 0;
        IReadOnlyDictionary<int, int[]>? first = null;

        Parallel.For(0, 64, _ =>
        {
            var value = cache.GetOrBuild(key, () =>
            {
                Interlocked.Increment(ref buildCount);
                Thread.Sleep(5);
                return new Dictionary<int, int[]>
                {
                    [1] = [101, 102],
                };
            });

            Interlocked.CompareExchange(ref first, value, comparand: null);
            ReferenceEquals(first, value).ShouldBeTrue();
        });

        buildCount.ShouldBe(1);
        first.ShouldNotBeNull();
        first![1].ShouldBe([101, 102]);
    }

    private sealed class IncludeContext(DbContextOptions<IncludeContext> options) : DbContext(options)
    {
        public DbSet<MapEntity> Maps => Set<MapEntity>();
        public DbSet<MapChallengeModeEntity> MapChallengeModes => Set<MapChallengeModeEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<MapEntity>(b =>
            {
                b.HasKey(x => x.Id);
                b.HasMany(x => x.MapChallengeModes)
                    .WithOne(x => x.Map)
                    .HasForeignKey(x => x.MapId);
            });

            modelBuilder.Entity<MapChallengeModeEntity>(b =>
            {
                b.HasKey(x => x.Id);
            });
        }
    }

    private sealed class MapEntity : Db2Entity<int>
    {
        public List<MapChallengeModeEntity> MapChallengeModes { get; set; } = [];
    }

    private sealed class MapChallengeModeEntity : Db2Entity<int>
    {
        public int MapId { get; set; }
        public MapEntity? Map { get; set; }
    }

    private sealed class TestDb2StreamProvider : IDb2StreamProvider
    {
        public Stream OpenDb2Stream(string tableName) => new NamedMemoryStream(tableName);

        public Task<Stream> OpenDb2StreamAsync(string tableName, CancellationToken cancellationToken = default)
            => Task.FromResult<Stream>(new NamedMemoryStream(tableName));
    }

    private sealed class NamedMemoryStream(string tableName) : MemoryStream
    {
        public string TableName { get; } = tableName;
    }

    private sealed class TestDb2Format(IReadOnlyDictionary<string, IDb2File> files) : IDb2Format
    {
        public Db2Format Format => Db2Format.Unknown;

        public IDb2File OpenFile(Stream stream)
        {
            if (stream is NamedMemoryStream { TableName: var name })
            {
                if (files.TryGetValue(name, out var file))
                {
                    return file;
                }

                throw new InvalidOperationException($"No test file registered for table '{name}'.");
            }

            throw new InvalidOperationException("Test format expects NamedMemoryStream.");
        }

        public Db2FileLayout GetLayout(IDb2File file)
            => new(layoutHash: file.Header.LayoutHash, physicalFieldsCount: file.Header.FieldsCount);

        public Db2FileLayout GetLayout(Stream stream)
            => throw new NotSupportedException();
    }

    private sealed class CountingDb2FileHeader(uint layoutHash, int fieldsCount) : IDb2FileHeader
    {
        public uint LayoutHash { get; } = layoutHash;
        public int FieldsCount { get; } = fieldsCount;
    }

    private sealed class CountingDb2File(
        IDb2FileHeader header,
        IReadOnlyList<RowHandle> rowHandles,
        IReadOnlyDictionary<(int RowId, int FieldIndex), object?> physicalFields) : IDb2File<RowHandle>
    {
        private readonly IReadOnlyList<RowHandle> _rowHandles = rowHandles;
        private readonly IReadOnlyDictionary<(int RowId, int FieldIndex), object?> _physicalFields = physicalFields;

        public IDb2FileHeader Header { get; } = header;
        public Type RowType => typeof(RowHandle);
        public Db2Flags Flags => 0;
        public int RecordsCount => _rowHandles.Count;
        public ReadOnlyMemory<byte> DenseStringTableBytes => ReadOnlyMemory<byte>.Empty;

        public int EnumerateRowHandlesCalls { get; private set; }
        public int TryGetRowByIdCalls { get; private set; }

        public IEnumerable<RowHandle> EnumerateRowHandles()
        {
            EnumerateRowHandlesCalls++;
            return _rowHandles;
        }

        public IEnumerable<RowHandle> EnumerateRows() => _rowHandles;

        public bool TryGetRowHandle<TId>(TId id, out RowHandle handle) where TId : IEquatable<TId>, IComparable<TId>
            => TryGetRowById(id, out handle);

        public bool TryGetRowById<TId>(TId id, out RowHandle row) where TId : IEquatable<TId>, IComparable<TId>
        {
            TryGetRowByIdCalls++;

            var requested = Convert.ToInt32(id);
            var found = _rowHandles.FirstOrDefault(h => h.RowId == requested);
            if (found.RowId == 0)
            {
                row = default;
                return false;
            }

            row = found;
            return true;
        }

        public T ReadField<T>(RowHandle handle, int fieldIndex)
        {
            if (fieldIndex == Db2VirtualFieldIndex.Id)
            {
                return (T)Convert.ChangeType(handle.RowId, typeof(T));
            }

            if (fieldIndex < 0)
            {
                throw new NotSupportedException($"Unsupported virtual field index {fieldIndex}.");
            }

            if (!_physicalFields.TryGetValue((handle.RowId, fieldIndex), out var value))
            {
                return default!;
            }

            if (value is null)
            {
                return default!;
            }

            if (value is T typed)
            {
                return typed;
            }

            return (T)Convert.ChangeType(value, typeof(T));
        }

        public void Dispose()
        {
        }
    }

    private sealed class TestDbdProvider(IReadOnlyDictionary<string, IDbdFile> files) : IDbdProvider
    {
        public IDbdFile Open(string tableName)
        {
            if (files.TryGetValue(tableName, out var file))
            {
                return file;
            }

            throw new InvalidOperationException($"No test DBD registered for table '{tableName}'.");
        }
    }

    private sealed class TestDbdFile : IDbdFile
    {
        public required IReadOnlyDictionary<string, IDbdColumn> ColumnsByName { get; init; }
        public required IReadOnlyList<IDbdLayout> Layouts { get; init; }
        public required IReadOnlyList<IDbdBuildBlock> GlobalBuilds { get; init; }

        public bool TryGetLayout(uint layoutHash, out IDbdLayout layout)
        {
            layout = default!;
            return false;
        }

        public static TestDbdFile Create(string buildLine, IReadOnlyList<IDbdLayoutEntry> entries)
        {
            return new TestDbdFile
            {
                ColumnsByName = new Dictionary<string, IDbdColumn>(StringComparer.Ordinal),
                Layouts = Array.Empty<IDbdLayout>(),
                GlobalBuilds =
                [
                    new TestDbdBuildBlock(buildLine, entries)
                ]
            };
        }
    }

    private sealed class TestDbdBuildBlock(string buildLine, IReadOnlyList<IDbdLayoutEntry> entries) : IDbdBuildBlock
    {
        public string BuildLine { get; } = buildLine;
        public IReadOnlyList<IDbdLayoutEntry> Entries { get; } = entries;

        public int GetPhysicalColumnCount()
            => Entries.Count(static e => !e.IsNonInline);
    }

    private sealed class TestDbdLayoutEntry(
        string name,
        Db2ValueType valueType,
        bool isNonInline = false,
        bool isId = false,
        bool isRelation = false,
        int elementCount = 1,
        string? referencedTableName = null) : IDbdLayoutEntry
    {
        public string Name { get; } = name;
        public Db2ValueType ValueType { get; } = valueType;
        public string? ReferencedTableName { get; } = referencedTableName;
        public int ElementCount { get; } = elementCount;
        public bool IsVerified => true;
        public bool IsNonInline { get; } = isNonInline;
        public bool IsId { get; } = isId;
        public bool IsRelation { get; } = isRelation;
        public string? InlineTypeToken => null;
    }
}
