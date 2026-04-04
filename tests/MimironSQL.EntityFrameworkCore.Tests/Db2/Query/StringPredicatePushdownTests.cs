using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

using Shouldly;

using MimironSQL.Db2;
using MimironSQL.Dbd;
using MimironSQL.EntityFrameworkCore.Infrastructure;
using MimironSQL.Formats;
using MimironSQL.Providers;

namespace MimironSQL.EntityFrameworkCore.Tests.Db2.Query;

public sealed class StringPredicatePushdownTests
{
    [Fact]
    public void StartsWith_with_constant_is_pushed_down()
    {
        var file = new StringTestDb2File(
            header: new TestDb2FileHeader(layoutHash: 0, fieldsCount: 2),
            rows:
            [
                new StringTestDb2Row(1, "Dire Maul", 1),
                new StringTestDb2Row(2, "Dalaran", 2),
                new StringTestDb2Row(3, "Stormwind", 3),
            ]);

        using var built = CreateContext<PushdownContext>(
            dbdFilesByTableName: new Dictionary<string, IDbdFile>(StringComparer.Ordinal)
            {
                [nameof(PushdownEntity)] = TestDbdFile.Create(
                    buildLine: TestHelpers.WowVersion,
                    entries:
                    [
                        new TestDbdLayoutEntry("Id", Db2ValueType.Int64, isNonInline: true, isId: true),
                        new TestDbdLayoutEntry("Name", Db2ValueType.String),
                        new TestDbdLayoutEntry("Value", Db2ValueType.Int64),
                    ])
            },
            filesByTableName: new Dictionary<string, IDb2File>(StringComparer.Ordinal)
            {
                [nameof(PushdownEntity)] = file,
            });

        var results = built.Context.Entities
            .AsNoTracking()
            .Where(e => e.Name.StartsWith("Dire"))
            .ToList();

        results.Select(static x => x.Id).ShouldBe([1]);
        file.GetReadFieldCalls(fieldIndex: 1).ShouldBe(1);
    }

    [Fact]
    public void EndsWith_with_constant_is_pushed_down()
    {
        var file = new StringTestDb2File(
            header: new TestDb2FileHeader(layoutHash: 0, fieldsCount: 2),
            rows:
            [
                new StringTestDb2Row(1, "abc_xyz", 1),
                new StringTestDb2Row(2, "abc", 2),
                new StringTestDb2Row(3, "xyz_abc", 3),
            ]);

        using var built = CreateContext<PushdownContext>(
            dbdFilesByTableName: new Dictionary<string, IDbdFile>(StringComparer.Ordinal)
            {
                [nameof(PushdownEntity)] = TestDbdFile.Create(
                    buildLine: TestHelpers.WowVersion,
                    entries:
                    [
                        new TestDbdLayoutEntry("Id", Db2ValueType.Int64, isNonInline: true, isId: true),
                        new TestDbdLayoutEntry("Name", Db2ValueType.String),
                        new TestDbdLayoutEntry("Value", Db2ValueType.Int64),
                    ])
            },
            filesByTableName: new Dictionary<string, IDb2File>(StringComparer.Ordinal)
            {
                [nameof(PushdownEntity)] = file,
            });

        var results = built.Context.Entities
            .AsNoTracking()
            .Where(e => e.Name.EndsWith("_xyz"))
            .ToList();

        results.Select(static x => x.Id).ShouldBe([1]);
        file.GetReadFieldCalls(fieldIndex: 1).ShouldBe(1);
    }

    [Fact]
    public void Contains_with_constant_is_pushed_down()
    {
        var file = new StringTestDb2File(
            header: new TestDb2FileHeader(layoutHash: 0, fieldsCount: 2),
            rows:
            [
                new StringTestDb2Row(1, "Dire Maul", 1),
                new StringTestDb2Row(2, "Dalaran", 2),
                new StringTestDb2Row(3, "Stormwind", 3),
            ]);

        using var built = CreateContext<PushdownContext>(
            dbdFilesByTableName: new Dictionary<string, IDbdFile>(StringComparer.Ordinal)
            {
                [nameof(PushdownEntity)] = TestDbdFile.Create(
                    buildLine: TestHelpers.WowVersion,
                    entries:
                    [
                        new TestDbdLayoutEntry("Id", Db2ValueType.Int64, isNonInline: true, isId: true),
                        new TestDbdLayoutEntry("Name", Db2ValueType.String),
                        new TestDbdLayoutEntry("Value", Db2ValueType.Int64),
                    ])
            },
            filesByTableName: new Dictionary<string, IDb2File>(StringComparer.Ordinal)
            {
                [nameof(PushdownEntity)] = file,
            });

        var results = built.Context.Entities
            .AsNoTracking()
            .Where(e => e.Name.Contains("ire"))
            .ToList();

        results.Select(static x => x.Id).ShouldBe([1]);
        file.GetReadFieldCalls(fieldIndex: 1).ShouldBe(1);
    }

    [Fact]
    public void StartsWith_with_ordinal_ignorecase_is_pushed_down()
    {
        var file = new StringTestDb2File(
            header: new TestDb2FileHeader(layoutHash: 0, fieldsCount: 2),
            rows:
            [
                new StringTestDb2Row(1, "Dire Maul", 1),
                new StringTestDb2Row(2, "Dalaran", 2),
                new StringTestDb2Row(3, "Stormwind", 3),
            ]);

        using var built = CreateContext<PushdownContext>(
            dbdFilesByTableName: new Dictionary<string, IDbdFile>(StringComparer.Ordinal)
            {
                [nameof(PushdownEntity)] = TestDbdFile.Create(
                    buildLine: TestHelpers.WowVersion,
                    entries:
                    [
                        new TestDbdLayoutEntry("Id", Db2ValueType.Int64, isNonInline: true, isId: true),
                        new TestDbdLayoutEntry("Name", Db2ValueType.String),
                        new TestDbdLayoutEntry("Value", Db2ValueType.Int64),
                    ])
            },
            filesByTableName: new Dictionary<string, IDb2File>(StringComparer.Ordinal)
            {
                [nameof(PushdownEntity)] = file,
            });

        var results = built.Context.Entities
            .AsNoTracking()
            .Where(e => e.Name.StartsWith("dire", StringComparison.OrdinalIgnoreCase))
            .ToList();

        results.Select(static x => x.Id).ShouldBe([1]);
        file.GetReadFieldCalls(fieldIndex: 1).ShouldBe(1);
    }

    [Fact]
    public void StartsWith_with_captured_variable_is_pushed_down()
    {
        var file = new StringTestDb2File(
            header: new TestDb2FileHeader(layoutHash: 0, fieldsCount: 2),
            rows:
            [
                new StringTestDb2Row(1, "Dire Maul", 1),
                new StringTestDb2Row(2, "Dalaran", 2),
                new StringTestDb2Row(3, "Stormwind", 3),
            ]);

        using var built = CreateContext<PushdownContext>(
            dbdFilesByTableName: new Dictionary<string, IDbdFile>(StringComparer.Ordinal)
            {
                [nameof(PushdownEntity)] = TestDbdFile.Create(
                    buildLine: TestHelpers.WowVersion,
                    entries:
                    [
                        new TestDbdLayoutEntry("Id", Db2ValueType.Int64, isNonInline: true, isId: true),
                        new TestDbdLayoutEntry("Name", Db2ValueType.String),
                        new TestDbdLayoutEntry("Value", Db2ValueType.Int64),
                    ])
            },
            filesByTableName: new Dictionary<string, IDb2File>(StringComparer.Ordinal)
            {
                [nameof(PushdownEntity)] = file,
            });

        var prefix = "Dire";

        var results = built.Context.Entities
            .AsNoTracking()
            .Where(e => e.Name.StartsWith(prefix))
            .ToList();

        results.Select(static x => x.Id).ShouldBe([1]);
        file.GetReadFieldCalls(fieldIndex: 1).ShouldBe(1);
    }

    [Fact]
    public void Entity_dependent_argument_falls_back_to_client_side_evaluation()
    {
        var file = new EntityDependentStringTestDb2File(
            header: new TestDb2FileHeader(layoutHash: 0, fieldsCount: 3),
            rows:
            [
                new EntityDependentStringTestDb2Row(1, "Dire Maul", "Di", 1),
                new EntityDependentStringTestDb2Row(2, "Dalaran", "Di", 2),
                new EntityDependentStringTestDb2Row(3, "Stormwind", "St", 3),
            ]);

        using var built = CreateContext<EntityDependentArgContext>(
            dbdFilesByTableName: new Dictionary<string, IDbdFile>(StringComparer.Ordinal)
            {
                [nameof(EntityDependentArgEntity)] = TestDbdFile.Create(
                    buildLine: TestHelpers.WowVersion,
                    entries:
                    [
                        new TestDbdLayoutEntry("Id", Db2ValueType.Int64, isNonInline: true, isId: true),
                        new TestDbdLayoutEntry("Name", Db2ValueType.String),
                        new TestDbdLayoutEntry("Value", Db2ValueType.Int64),
                        new TestDbdLayoutEntry("Prefix", Db2ValueType.String),
                    ])
            },
            filesByTableName: new Dictionary<string, IDb2File>(StringComparer.Ordinal)
            {
                [nameof(EntityDependentArgEntity)] = file,
            });

        var results = built.Context.Entities
            .AsNoTracking()
            .Where(e => e.Name.StartsWith(e.Prefix))
            .ToList();

        results.Select(static x => x.Id).ShouldBe([1, 3]);

        // If translation fails, the engine materializes all rows and filters on the client.
        file.GetReadFieldCalls(fieldIndex: 1).ShouldBe(3);
    }

    [Fact]
    public void Unsupported_stringcomparison_falls_back_to_client_side_evaluation()
    {
        var file = new StringTestDb2File(
            header: new TestDb2FileHeader(layoutHash: 0, fieldsCount: 2),
            rows:
            [
                new StringTestDb2Row(1, "Dire Maul", 1),
                new StringTestDb2Row(2, "Dalaran", 2),
                new StringTestDb2Row(3, "Stormwind", 3),
            ]);

        using var built = CreateContext<PushdownContext>(
            dbdFilesByTableName: new Dictionary<string, IDbdFile>(StringComparer.Ordinal)
            {
                [nameof(PushdownEntity)] = TestDbdFile.Create(
                    buildLine: TestHelpers.WowVersion,
                    entries:
                    [
                        new TestDbdLayoutEntry("Id", Db2ValueType.Int64, isNonInline: true, isId: true),
                        new TestDbdLayoutEntry("Name", Db2ValueType.String),
                        new TestDbdLayoutEntry("Value", Db2ValueType.Int64),
                    ])
            },
            filesByTableName: new Dictionary<string, IDb2File>(StringComparer.Ordinal)
            {
                [nameof(PushdownEntity)] = file,
            });

        var results = built.Context.Entities
            .AsNoTracking()
            .Where(e => e.Name.StartsWith("Dire", StringComparison.CurrentCulture))
            .ToList();

        results.Select(static x => x.Id).ShouldBe([1]);
        file.GetReadFieldCalls(fieldIndex: 1).ShouldBe(3);
    }

    [Fact]
    public void Integration_querying_map_name_startswith_returns_correct_results()
    {
        var file = new StringTestDb2File(
            header: new TestDb2FileHeader(layoutHash: 0, fieldsCount: 2),
            rows:
            [
                new StringTestDb2Row(1, "Dire Maul", 1),
                new StringTestDb2Row(2, "Dalaran", 2),
                new StringTestDb2Row(3, "Stormwind", 3),
            ]);

        using var built = CreateContext<MapContext>(
            dbdFilesByTableName: new Dictionary<string, IDbdFile>(StringComparer.Ordinal)
            {
                [nameof(Map)] = TestDbdFile.Create(
                    buildLine: TestHelpers.WowVersion,
                    entries:
                    [
                        new TestDbdLayoutEntry("Id", Db2ValueType.Int64, isNonInline: true, isId: true),
                        new TestDbdLayoutEntry("Name", Db2ValueType.String),
                        new TestDbdLayoutEntry("Value", Db2ValueType.Int64),
                    ])
            },
            filesByTableName: new Dictionary<string, IDb2File>(StringComparer.Ordinal)
            {
                [nameof(Map)] = file,
            });

        var results = built.Context.Set<Map>()
            .AsNoTracking()
            .Where(m => m.Name.StartsWith("Dire"))
            .ToList();

        results.Select(static x => x.Id).ShouldBe([1]);
        results.All(static x => x.Name.StartsWith("Dire", StringComparison.Ordinal)).ShouldBeTrue();
    }

    private static BuiltContext<TContext> CreateContext<TContext>(
        IReadOnlyDictionary<string, IDbdFile> dbdFilesByTableName,
        IReadOnlyDictionary<string, IDb2File> filesByTableName)
        where TContext : DbContext
    {
        var optionsBuilder = new DbContextOptionsBuilder<TContext>();

        var dbdProvider = new TestDbdProvider(dbdFilesByTableName);
        var streamProvider = new TestDb2StreamProvider();
        var format = new TestDb2Format(filesByTableName);

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

    private sealed class PushdownContext(DbContextOptions<PushdownContext> options) : DbContext(options)
    {
        public DbSet<PushdownEntity> Entities => Set<PushdownEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<PushdownEntity>(b =>
            {
                b.HasKey(x => x.Id);
                b.Property(x => x.Name);
                b.Property(x => x.Value);
            });
        }
    }

    private sealed class EntityDependentArgContext(DbContextOptions<EntityDependentArgContext> options) : DbContext(options)
    {
        public DbSet<EntityDependentArgEntity> Entities => Set<EntityDependentArgEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<EntityDependentArgEntity>(b =>
            {
                b.HasKey(x => x.Id);
                b.Property(x => x.Name);
                b.Property(x => x.Value);
                b.Property(x => x.Prefix);
            });
        }
    }

    private sealed class MapContext(DbContextOptions<MapContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Map>(b =>
            {
                b.HasKey(x => x.Id);
                b.Property(x => x.Name);
                b.Property(x => x.Value);
            });
        }
    }

    private sealed class PushdownEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Value { get; set; }
    }

    private sealed class EntityDependentArgEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Value { get; set; }
        public string Prefix { get; set; } = "";
    }

    private sealed class Map
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Value { get; set; }
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

    private sealed class StringTestDb2File : IDb2File<RowHandle>
    {
        private readonly IReadOnlyList<RowHandle> _handles;
        private readonly IReadOnlyDictionary<int, StringTestDb2Row> _rowsById;
        private readonly Dictionary<int, int> _readFieldCalls = new();

        public StringTestDb2File(IDb2FileHeader header, IReadOnlyList<StringTestDb2Row> rows)
        {
            Header = header;
            _handles = rows.Select(static r => new RowHandle(0, 0, r.Id)).ToArray();
            _rowsById = rows.ToDictionary(static r => r.Id);
        }

        public IDb2FileHeader Header { get; }
        public Type RowType => typeof(RowHandle);
        public Db2Flags Flags => 0;
        public int RecordsCount => _handles.Count;
        public ReadOnlyMemory<byte> DenseStringTableBytes => ReadOnlyMemory<byte>.Empty;

        public int GetReadFieldCalls(int fieldIndex)
            => _readFieldCalls.TryGetValue(fieldIndex, out var count) ? count : 0;

        public IEnumerable<RowHandle> EnumerateRowHandles() => _handles;

        public IEnumerable<RowHandle> EnumerateRows() => _handles;

        public bool TryGetRowHandle<TId>(TId id, out RowHandle handle) where TId : IEquatable<TId>, IComparable<TId>
        {
            var requested = Convert.ToInt32(id);
            if (_rowsById.ContainsKey(requested))
            {
                handle = new RowHandle(0, 0, requested);
                return true;
            }

            handle = default;
            return false;
        }

        public bool TryGetRowById<TId>(TId id, out RowHandle row) where TId : IEquatable<TId>, IComparable<TId>
        {
            var requested = Convert.ToInt32(id);
            if (_rowsById.ContainsKey(requested))
            {
                row = new RowHandle(0, 0, requested);
                return true;
            }

            row = default;
            return false;
        }

        public T ReadField<T>(RowHandle handle, int fieldIndex)
        {
            _readFieldCalls[fieldIndex] = GetReadFieldCalls(fieldIndex) + 1;

            if (fieldIndex == Db2VirtualFieldIndex.Id)
            {
                return (T)Convert.ChangeType(handle.RowId, typeof(T));
            }

            if (!_rowsById.TryGetValue(handle.RowId, out var row))
            {
                return default!;
            }

            object? value = fieldIndex switch
            {
                0 => row.Name,
                1 => row.Value,
                _ => throw new NotSupportedException($"Unsupported field index {fieldIndex}."),
            };

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

    private sealed class EntityDependentStringTestDb2File : IDb2File<RowHandle>
    {
        private readonly IReadOnlyList<RowHandle> _handles;
        private readonly IReadOnlyDictionary<int, EntityDependentStringTestDb2Row> _rowsById;
        private readonly Dictionary<int, int> _readFieldCalls = new();

        public EntityDependentStringTestDb2File(IDb2FileHeader header, IReadOnlyList<EntityDependentStringTestDb2Row> rows)
        {
            Header = header;
            _handles = rows.Select(static r => new RowHandle(0, 0, r.Id)).ToArray();
            _rowsById = rows.ToDictionary(static r => r.Id);
        }

        public IDb2FileHeader Header { get; }
        public Type RowType => typeof(RowHandle);
        public Db2Flags Flags => 0;
        public int RecordsCount => _handles.Count;
        public ReadOnlyMemory<byte> DenseStringTableBytes => ReadOnlyMemory<byte>.Empty;

        public int GetReadFieldCalls(int fieldIndex)
            => _readFieldCalls.TryGetValue(fieldIndex, out var count) ? count : 0;

        public IEnumerable<RowHandle> EnumerateRowHandles() => _handles;

        public IEnumerable<RowHandle> EnumerateRows() => _handles;

        public bool TryGetRowHandle<TId>(TId id, out RowHandle handle) where TId : IEquatable<TId>, IComparable<TId>
        {
            var requested = Convert.ToInt32(id);
            if (_rowsById.ContainsKey(requested))
            {
                handle = new RowHandle(0, 0, requested);
                return true;
            }

            handle = default;
            return false;
        }

        public bool TryGetRowById<TId>(TId id, out RowHandle row) where TId : IEquatable<TId>, IComparable<TId>
        {
            var requested = Convert.ToInt32(id);
            if (_rowsById.ContainsKey(requested))
            {
                row = new RowHandle(0, 0, requested);
                return true;
            }

            row = default;
            return false;
        }

        public T ReadField<T>(RowHandle handle, int fieldIndex)
        {
            _readFieldCalls[fieldIndex] = GetReadFieldCalls(fieldIndex) + 1;

            if (fieldIndex == Db2VirtualFieldIndex.Id)
            {
                return (T)Convert.ChangeType(handle.RowId, typeof(T));
            }

            if (!_rowsById.TryGetValue(handle.RowId, out var row))
            {
                return default!;
            }

            object? value = fieldIndex switch
            {
                0 => row.Name,
                1 => row.Value,
                2 => row.Prefix,
                _ => throw new NotSupportedException($"Unsupported field index {fieldIndex}."),
            };

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

    private sealed record StringTestDb2Row(int Id, string Name, int Value);

    private sealed record EntityDependentStringTestDb2Row(int Id, string Name, string Prefix, int Value);

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
