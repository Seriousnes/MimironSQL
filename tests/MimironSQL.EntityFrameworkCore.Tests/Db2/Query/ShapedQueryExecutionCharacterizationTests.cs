using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

using Shouldly;

using MimironSQL.Db2;
using MimironSQL.Dbd;
using MimironSQL.EntityFrameworkCore.Infrastructure;
using MimironSQL.Formats;
using MimironSQL.Providers;

namespace MimironSQL.EntityFrameworkCore.Tests.Db2.Query;

public sealed class ShapedQueryExecutionCharacterizationTests
{
    [Fact]
    public void Where_pushdown_skips_valuebuffer_reads_for_filtered_out_rows()
    {
        var dbdProvider = new TestDbdProvider(new Dictionary<string, IDbdFile>(StringComparer.Ordinal)
        {
            [nameof(TrackedEntity)] = TestDbdFile.Create(
                buildLine: TestHelpers.WowVersion,
                entries:
                [
                    new TestDbdLayoutEntry("Id", Db2ValueType.Int64, isNonInline: true, isId: true),
                    new TestDbdLayoutEntry("Name", Db2ValueType.String)
                ])
        });

        var file = new TestDb2File(
            header: new TestDb2FileHeader(layoutHash: 0, fieldsCount: 1),
            rowHandles:
            [
                new RowHandle(0, 0, rowId: 1),
                new RowHandle(0, 0, rowId: 2),
                new RowHandle(0, 0, rowId: 3),
            ],
            physicalFields: new Dictionary<int, object?>
            {
                [0] = "hello"
            });

        var streamProvider = new TestDb2StreamProvider();
        var format = new TestDb2Format(new Dictionary<string, IDb2File>(StringComparer.Ordinal)
        {
            [nameof(TrackedEntity)] = file
        });

        using var built = CreateContext<TrackedContext>(dbdProvider, streamProvider, format);
        var context = built.Context;

        var results = context.Entities
            .AsNoTracking()
            .Where(e => e.Id > 2)
            .ToList();

        results.Count.ShouldBe(1);
        file.GetReadFieldCalls(fieldIndex: 0).ShouldBe(1);
    }

    [Fact]
    public void Find_on_virtual_id_primary_key_uses_TryGetRowById_and_does_not_enumerate_handles()
    {
        var dbdProvider = new TestDbdProvider(new Dictionary<string, IDbdFile>(StringComparer.Ordinal)
        {
            [nameof(TrackedEntity)] = TestDbdFile.Create(
                buildLine: TestHelpers.WowVersion,
                entries:
                [
                    new TestDbdLayoutEntry("Id", Db2ValueType.Int64, isNonInline: true, isId: true),
                    new TestDbdLayoutEntry("Name", Db2ValueType.String)
                ])
        });

        var file = new TestDb2File(
            header: new TestDb2FileHeader(layoutHash: 0, fieldsCount: 1),
            rowHandles:
            [
                new RowHandle(0, 0, rowId: 1),
                new RowHandle(0, 0, rowId: 2),
                new RowHandle(0, 0, rowId: 3),
            ],
            physicalFields: new Dictionary<int, object?>
            {
                [0] = "hello"
            });

        var streamProvider = new TestDb2StreamProvider();
        var format = new TestDb2Format(new Dictionary<string, IDb2File>(StringComparer.Ordinal)
        {
            [nameof(TrackedEntity)] = file
        });

        using var built = CreateContext<TrackedContext>(dbdProvider, streamProvider, format);
        var context = built.Context;

        var found = context.Entities.Find(2);
        found.ShouldNotBeNull();
        found!.Id.ShouldBe(2);

        file.TryGetRowByIdCalls.ShouldBe(1);
        file.EnumerateRowHandlesCalls.ShouldBe(0);
    }

    [Fact]
    public void Tracked_entity_query_throws_NullReferenceException_in_TryGetEntry()
    {
        var dbdProvider = new TestDbdProvider(new Dictionary<string, IDbdFile>(StringComparer.Ordinal)
        {
            [nameof(TrackedEntity)] = TestDbdFile.Create(
                buildLine: TestHelpers.WowVersion,
                entries:
                [
                    new TestDbdLayoutEntry("Id", Db2ValueType.Int64, isNonInline: true, isId: true),
                    new TestDbdLayoutEntry("Name", Db2ValueType.String)
                ])
        });

        var file = new TestDb2File(
            header: new TestDb2FileHeader(layoutHash: 0, fieldsCount: 1),
            rowHandles: [new RowHandle(0, 0, rowId: 1)],
            physicalFields: new Dictionary<int, object?>
            {
                [0] = "hello"
            });

        var streamProvider = new TestDb2StreamProvider();
        var format = new TestDb2Format(new Dictionary<string, IDb2File>(StringComparer.Ordinal)
        {
            [nameof(TrackedEntity)] = file
        });

        using var built = CreateContext<TrackedContext>(dbdProvider, streamProvider, format);
        var context = built.Context;

        var results = context.Entities.Take(1).ToList();
        results.Count.ShouldBe(1);
    }

    [Fact]
    public void Walking_back_include_tree_throws_when_NavigationBaseIncludeIgnored_is_configured_as_error()
    {
        var dbdProvider = new TestDbdProvider(new Dictionary<string, IDbdFile>(StringComparer.Ordinal)
        {
            [nameof(MapChallengeModeEntity)] = TestDbdFile.Create(
                buildLine: TestHelpers.WowVersion,
                entries:
                [
                    new TestDbdLayoutEntry("Id", Db2ValueType.Int64, isNonInline: true, isId: true),
                    new TestDbdLayoutEntry("MapId", Db2ValueType.Int64),
                ]),
            [nameof(MapEntity)] = TestDbdFile.Create(
                buildLine: TestHelpers.WowVersion,
                entries:
                [
                    new TestDbdLayoutEntry("Id", Db2ValueType.Int64, isNonInline: true, isId: true),
                ])
        });

        var streamProvider = new TestDb2StreamProvider();
        var format = new TestDb2Format(new Dictionary<string, IDb2File>(StringComparer.Ordinal)
        {
            // Enumerate zero rows so we don't ever execute includes if compilation succeeds.
            [nameof(MapChallengeModeEntity)] = new TestDb2File(
                header: new TestDb2FileHeader(layoutHash: 0, fieldsCount: 1),
                rowHandles: [],
                physicalFields: new Dictionary<int, object?>()),
            [nameof(MapEntity)] = new TestDb2File(
                header: new TestDb2FileHeader(layoutHash: 0, fieldsCount: 0),
                rowHandles: [],
                physicalFields: new Dictionary<int, object?>())
        });

        using var built = CreateContext<MapChallengeContext>(dbdProvider, streamProvider, format, options =>
            options.ConfigureWarnings(w => w.Throw(CoreEventId.NavigationBaseIncludeIgnored)));

        var context = built.Context;

        Should.Throw<InvalidOperationException>(() =>
            context.MapChallengeModes
                .AsNoTracking()
                .Include(x => x.Map)
                .ThenInclude(x => x.MapChallengeModes)
                .ThenInclude(x => x.Map)
                .Take(0)
                .ToList());
    }

    [Fact]
    public void Unsupported_scalar_type_thrown_from_ReadField_is_thrown_as_NotSupportedException()
    {
        var dbdProvider = new TestDbdProvider(new Dictionary<string, IDbdFile>(StringComparer.Ordinal)
        {
            [nameof(BadScalarEntity)] = TestDbdFile.Create(
                buildLine: TestHelpers.WowVersion,
                entries:
                [
                    new TestDbdLayoutEntry("Id", Db2ValueType.Int64, isNonInline: true, isId: true),
                    new TestDbdLayoutEntry("Bad", Db2ValueType.Int64)
                ])
        });

        var file = new TestDb2File(
            header: new TestDb2FileHeader(layoutHash: 0, fieldsCount: 1),
            rowHandles: [new RowHandle(0, 0, rowId: 1)],
            physicalFields: new Dictionary<int, object?>
            {
                [0] = 123
            },
            throwOnReadTypes: [typeof(ICollection<int>)],
            throwMessage: "Unsupported scalar type System.Collections.Generic.ICollection`1[[System.Int32, System.Private.CoreLib, Version=10.0.0.0, Culture=neutral, PublicKeyToken=7cec85d7bea7798e]].");

        var streamProvider = new TestDb2StreamProvider();
        var format = new TestDb2Format(new Dictionary<string, IDb2File>(StringComparer.Ordinal)
        {
            [nameof(BadScalarEntity)] = file
        });

        using var built = CreateContext<BadScalarContext>(dbdProvider, streamProvider, format);
        var context = built.Context;

        var ex = Should.Throw<NotSupportedException>(() =>
            context.BadScalars
                .AsNoTracking()
                .Take(1)
                .ToList());

        ex.Message.ShouldBe("Unsupported scalar type System.Collections.Generic.ICollection`1[[System.Int32, System.Private.CoreLib, Version=10.0.0.0, Culture=neutral, PublicKeyToken=7cec85d7bea7798e]].");
    }

    private static BuiltContext<TContext> CreateContext<TContext>(
        IDbdProvider dbdProvider,
        IDb2StreamProvider streamProvider,
        IDb2Format format,
        Action<DbContextOptionsBuilder<TContext>>? configureOptions = null)
        where TContext : DbContext
    {
        var optionsBuilder = new DbContextOptionsBuilder<TContext>();

        optionsBuilder.UseMimironDb2ForTests(o =>
        {
            o.ConfigureProvider(
                providerKey: "test",
                providerConfigHash: 0,
                applyProviderServices: services =>
                {
                    services.AddSingleton(dbdProvider);
                    services.AddSingleton(streamProvider);
                    services.AddSingleton(format);
                });
        });

        configureOptions?.Invoke(optionsBuilder);

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();

        var services = new ServiceCollection();
        extension!.ApplyServices(services);

        var serviceProvider = services.BuildServiceProvider();
        optionsBuilder.UseInternalServiceProvider(serviceProvider);

        var context = (TContext)Activator.CreateInstance(typeof(TContext), optionsBuilder.Options)!;
        return new BuiltContext<TContext>(context, serviceProvider);
    }

    private sealed record BuiltContext<TContext>(TContext Context, ServiceProvider ServiceProvider) : IDisposable
        where TContext : DbContext
    {
        public void Dispose()
        {
            Context.Dispose();
            ServiceProvider.Dispose();
        }
    }

    private sealed class TrackedContext(DbContextOptions<TrackedContext> options) : DbContext(options)
    {
        public DbSet<TrackedEntity> Entities => Set<TrackedEntity>();
    }

    private sealed class MapChallengeContext(DbContextOptions<MapChallengeContext> options) : DbContext(options)
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

    private sealed class BadScalarContext(DbContextOptions<BadScalarContext> options) : DbContext(options)
    {
        public DbSet<BadScalarEntity> BadScalars => Set<BadScalarEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<BadScalarEntity>(b =>
            {
                b.HasKey(x => x.Id);
                b.Property(x => x.Bad);
            });
        }
    }

    private sealed class TrackedEntity
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    private sealed class MapEntity
    {
        public int Id { get; set; }
        public List<MapChallengeModeEntity> MapChallengeModes { get; set; } = [];
    }

    private sealed class MapChallengeModeEntity
    {
        public int Id { get; set; }
        public int MapId { get; set; }
        public MapEntity? Map { get; set; }
    }

    private sealed class BadScalarEntity
    {
        public int Id { get; set; }
        public ICollection<int> Bad { get; set; } = [];
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

    private sealed class TestDb2File : IDb2File<RowHandle>
    {
        private readonly IReadOnlyList<RowHandle> _rowHandles;
        private readonly IReadOnlyDictionary<int, object?> _physicalFields;
        private readonly HashSet<Type> _throwOnReadTypes;
        private readonly string? _throwMessage;

        private readonly Dictionary<int, int> _readFieldCalls = new();

        public TestDb2File(
            IDb2FileHeader header,
            IReadOnlyList<RowHandle> rowHandles,
            IReadOnlyDictionary<int, object?> physicalFields,
            IReadOnlyCollection<Type>? throwOnReadTypes = null,
            string? throwMessage = null)
        {
            Header = header;
            _rowHandles = rowHandles;
            _physicalFields = physicalFields;
            _throwOnReadTypes = throwOnReadTypes is null ? [] : new HashSet<Type>(throwOnReadTypes);
            _throwMessage = throwMessage;
        }

        public IDb2FileHeader Header { get; }
        public Type RowType => typeof(RowHandle);
        public Db2Flags Flags => 0;
        public int RecordsCount => _rowHandles.Count;
        public ReadOnlyMemory<byte> DenseStringTableBytes => ReadOnlyMemory<byte>.Empty;

        public int EnumerateRowHandlesCalls { get; private set; }
        public int TryGetRowByIdCalls { get; private set; }

        public int GetReadFieldCalls(int fieldIndex)
            => _readFieldCalls.TryGetValue(fieldIndex, out var count) ? count : 0;

        public IEnumerable<RowHandle> EnumerateRowHandles()
        {
            EnumerateRowHandlesCalls++;
            return _rowHandles;
        }

        public IEnumerable<RowHandle> EnumerateRows() => _rowHandles;

        public bool TryGetRowHandle<TId>(TId id, out RowHandle handle) where TId : IEquatable<TId>, IComparable<TId>
        {
            var requested = Convert.ToInt32(id);
            var found = _rowHandles.FirstOrDefault(h => h.RowId == requested);
            if (found.RowId == 0)
            {
                handle = default;
                return false;
            }

            handle = found;
            return true;
        }

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
            _readFieldCalls[fieldIndex] = GetReadFieldCalls(fieldIndex) + 1;

            if (_throwOnReadTypes.Contains(typeof(T)))
            {
                throw new NotSupportedException(_throwMessage ?? $"Unsupported scalar type {typeof(T).FullName}.");
            }

            if (fieldIndex == Db2VirtualFieldIndex.Id)
            {
                return (T)Convert.ChangeType(handle.RowId, typeof(T));
            }

            if (fieldIndex < 0)
            {
                throw new NotSupportedException($"Unsupported virtual field index {fieldIndex}.");
            }

            if (!_physicalFields.TryGetValue(fieldIndex, out var value))
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
