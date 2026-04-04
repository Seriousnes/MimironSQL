using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

using Shouldly;

using MimironSQL.Db2;
using MimironSQL.Dbd;
using MimironSQL.EntityFrameworkCore.Infrastructure;
using MimironSQL.Formats;
using MimironSQL.Providers;

namespace MimironSQL.EntityFrameworkCore.Tests.Db2.Query;

public sealed class OrderingAndPaginationTests(OrderingAndPaginationTests.Fixture fixture) : IClassFixture<OrderingAndPaginationTests.Fixture>
{
    private readonly Fixture _fixture = fixture;

    [Fact]
    public void OrderBy_ThenBy_Take_returns_sorted_results()
    {
        var results = _fixture.Context.Entities
            .AsNoTracking()
            .OrderBy(e => e.Name)
            .ThenBy(e => e.Id)
            .Take(4)
            .Select(e => e.Id)
            .ToList();

        results.ShouldBe([2, 4, 1, 5]);
    }

    [Fact]
    public void OrderBy_resets_previous_orderings()
    {
        var results = _fixture.Context.Entities
            .AsNoTracking()
            .OrderBy(e => e.Id)
            .OrderByDescending(e => e.Name)
            .ThenBy(e => e.Id)
            .Select(e => e.Id)
            .ToList();

        results.ShouldBe([3, 1, 5, 2, 4]);
    }

    [Fact]
    public void OrderBy_Skip_Take_paginates_results()
    {
        var results = _fixture.Context.Entities
            .AsNoTracking()
            .OrderBy(e => e.Id)
            .Skip(2)
            .Take(2)
            .Select(e => e.Id)
            .ToList();

        results.ShouldBe([3, 4]);
    }

    [Fact]
    public void Parameterized_Skip_is_supported()
    {
        var skip = 2;

        var results = _fixture.Context.Entities
            .AsNoTracking()
            .OrderBy(e => e.Id)
            .Skip(skip)
            .Take(2)
            .Select(e => e.Id)
            .ToList();

        results.ShouldBe([3, 4]);
    }

    [Fact]
    public void Multiple_Skip_calls_are_cumulative()
    {
        var results = _fixture.Context.Entities
            .AsNoTracking()
            .OrderBy(e => e.Id)
            .Skip(1)
            .Skip(2)
            .Take(10)
            .Select(e => e.Id)
            .ToList();

        results.ShouldBe([4, 5]);
    }

    [Fact]
    public void Skip_without_OrderBy_uses_natural_file_order()
    {
        var results = _fixture.Context.Entities
            .AsNoTracking()
            .Skip(2)
            .Take(2)
            .Select(e => e.Id)
            .ToList();

        results.ShouldBe([2, 5]);
    }

    [Fact]
    public void Ordering_after_Select_orders_by_projected_shape()
    {
        var results = _fixture.Context.Entities
            .AsNoTracking()
            .Select(e => new { e.Id, e.Name })
            .OrderBy(x => x.Name)
            .ThenBy(x => x.Id)
            .Take(4)
            .Select(x => x.Id)
            .ToList();

        results.ShouldBe([2, 4, 1, 5]);
    }

    [Fact]
    public void OrderByDescending_ThenBy_applies_compound_ordering()
    {
        var results = _fixture.Context.Entities
            .AsNoTracking()
            .OrderByDescending(e => e.Name)
            .ThenBy(e => e.Id)
            .Select(e => e.Id)
            .ToList();

        results.ShouldBe([3, 1, 5, 2, 4]);
    }

    [Fact]
    public void Ordered_LastOrDefault_uses_ordering_reversal()
    {
        var found = _fixture.Context.Entities
            .AsNoTracking()
            .OrderBy(e => e.Id)
            .LastOrDefault();

        found.ShouldNotBeNull();
        found!.Id.ShouldBe(5);
    }

    [Fact]
    public void Ordered_LastOrDefault_with_descending_order_returns_first_in_ascending()
    {
        var found = _fixture.Context.Entities
            .AsNoTracking()
            .OrderByDescending(e => e.Id)
            .LastOrDefault();

        found.ShouldNotBeNull();
        found!.Id.ShouldBe(1);
    }

    [Fact]
    public void Unordered_LastOrDefault_uses_natural_file_order()
    {
        var found = _fixture.Context.Entities
            .AsNoTracking()
            .LastOrDefault();

        found.ShouldNotBeNull();
        found!.Id.ShouldBe(4);
    }

    public sealed class Fixture : IDisposable
    {
        public Fixture()
        {
            var optionsBuilder = new DbContextOptionsBuilder<OrderingContext>();

            var dbdProvider = new TestDbdProvider(new Dictionary<string, IDbdFile>(StringComparer.Ordinal)
            {
                [nameof(OrderingEntity)] = TestDbdFile.Create(
                    buildLine: TestHelpers.WowVersion,
                    entries:
                    [
                        new TestDbdLayoutEntry("Id", Db2ValueType.Int64, isNonInline: true, isId: true),
                        new TestDbdLayoutEntry("Name", Db2ValueType.String)
                    ])
            });

            var file = new OrderingTestDb2File(
                header: new TestDb2FileHeader(layoutHash: 0, fieldsCount: 1),
                rowHandles:
                [
                    new RowHandle(0, 0, rowId: 3),
                    new RowHandle(0, 0, rowId: 1),
                    new RowHandle(0, 0, rowId: 2),
                    new RowHandle(0, 0, rowId: 5),
                    new RowHandle(0, 0, rowId: 4),
                ],
                namesByRowId: new Dictionary<int, string>(capacity: 5)
                {
                    [1] = "b",
                    [2] = "a",
                    [3] = "c",
                    [4] = "a",
                    [5] = "b",
                });

            var streamProvider = new TestDb2StreamProvider();
            var format = new TestDb2Format(new Dictionary<string, IDb2File>(StringComparer.Ordinal)
            {
                [nameof(OrderingEntity)] = file
            });

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

            ServiceProvider = services.BuildServiceProvider();
            optionsBuilder.UseInternalServiceProvider(ServiceProvider);

            Context = new OrderingContext(optionsBuilder.Options);
        }

        internal OrderingContext Context { get; }
        private ServiceProvider ServiceProvider { get; }

        public void Dispose()
        {
            Context.Dispose();
            ServiceProvider.Dispose();
        }

        internal sealed class OrderingContext(DbContextOptions<OrderingContext> options) : DbContext(options)
        {
            public DbSet<OrderingEntity> Entities => Set<OrderingEntity>();

            protected override void OnModelCreating(ModelBuilder modelBuilder)
            {
                modelBuilder.Entity<OrderingEntity>(b =>
                {
                    b.HasKey(e => e.Id);
                    b.Property(e => e.Name);
                });
            }
        }

        internal sealed class OrderingEntity
        {
            public int Id { get; set; }
            public string? Name { get; set; }
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

        private sealed class TestDb2FileHeader(uint layoutHash, int fieldsCount) : IDb2FileHeader
        {
            public uint LayoutHash { get; } = layoutHash;
            public int FieldsCount { get; } = fieldsCount;
        }

        private sealed class OrderingTestDb2File : IDb2File<RowHandle>
        {
            private readonly IReadOnlyList<RowHandle> _rowHandles;
            private readonly IReadOnlyDictionary<int, string> _namesByRowId;

            public OrderingTestDb2File(
                IDb2FileHeader header,
                IReadOnlyList<RowHandle> rowHandles,
                IReadOnlyDictionary<int, string> namesByRowId)
            {
                Header = header;
                _rowHandles = rowHandles;
                _namesByRowId = namesByRowId;
            }

            public IDb2FileHeader Header { get; }
            public Type RowType => typeof(RowHandle);
            public Db2Flags Flags => 0;
            public int RecordsCount => _rowHandles.Count;
            public ReadOnlyMemory<byte> DenseStringTableBytes => ReadOnlyMemory<byte>.Empty;

            public IEnumerable<RowHandle> EnumerateRowHandles() => _rowHandles;

            public IEnumerable<RowHandle> EnumerateRows() => _rowHandles;

            public T ReadField<T>(RowHandle handle, int fieldIndex)
            {
                if (fieldIndex == Db2VirtualFieldIndex.Id)
                {
                    return (T)Convert.ChangeType(handle.RowId, typeof(T));
                }

                if (fieldIndex == 0)
                {
                    if (!_namesByRowId.TryGetValue(handle.RowId, out var name))
                    {
                        return default!;
                    }

                    return (T)Convert.ChangeType(name, typeof(T));
                }

                if (fieldIndex < 0)
                {
                    throw new NotSupportedException($"Unsupported virtual field index {fieldIndex}.");
                }

                return default!;
            }

            public bool TryGetRowHandle<TId>(TId id, out RowHandle handle)
                where TId : IEquatable<TId>, IComparable<TId>
            {
                var requested = Convert.ToInt32(id);
                handle = _rowHandles.FirstOrDefault(h => h.RowId == requested);
                return handle.RowId != 0;
            }

            public bool TryGetRowById<TId>(TId id, out RowHandle row)
                where TId : IEquatable<TId>, IComparable<TId>
                => TryGetRowHandle(id, out row);

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
            bool isId = false) : IDbdLayoutEntry
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
}
