using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.Formats;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2IncludeChainExecutorTests
{
    [Fact]
    public void Apply_populates_foreign_key_array_collection_include_when_targets_exist()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<ParentFkArray>()
            .HasMany(x => x.Children)
            .WithForeignKeyArray<Key>(x => x.ChildIds);

        builder.Entity<ParentFkArray>().HasKey(x => x.Id);
        builder.Entity<ChildFkArray>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolverFkArray);

        var parent = new ParentFkArray
        {
            Id = 10,
            ChildIds = [Key.One, Key.Two, Key.None],
            Children = [],
        };

        var childrenFile = InMemoryDb2File.Create(
            rows:
            [
                new InMemoryDb2File.Row(1, [1]),
                new InMemoryDb2File.Row(2, [2]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(ChildFkArray) => (childrenFile, SchemaResolverFkArray(tableName)),
                _ => (new EmptyDb2File(), SchemaResolverFkArray(tableName)),
            };

        Db2IncludeChainExecutor.Apply<ParentFkArray, RowHandle>(
            source: [parent],
            model: model,
            tableResolver: TableResolver,
            members: [typeof(ParentFkArray).GetProperty(nameof(ParentFkArray.Children))!],
            entityFactory: new ReflectionDb2EntityFactory())
            .ShouldNotBeNull();

        parent.Children.Select(c => c.Id).ToArray().ShouldBe([1, 2]);
    }

    [Fact]
    public void Apply_foreign_key_array_throws_for_string_enumerable_key_member()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<ParentStringKeys>()
            .HasMany(x => x.Children)
            .WithForeignKeyArray<char>(x => x.ChildIds);

        builder.Entity<ParentStringKeys>().HasKey(x => x.Id);
        builder.Entity<ChildStringKeys>().HasKey(x => x.Id);

        var ex = Should.Throw<NotSupportedException>(() => builder.Build(SchemaResolverStringKeys));
        ex.Message.ShouldContain("expects an integer key collection");
    }

    [Fact]
    public void Apply_populates_dependent_foreign_key_collection_include()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<ParentDependent>()
            .HasMany(x => x.Children)
            .WithForeignKey(x => x.ParentId)
            .HasPrincipalKey(x => x.Id);

        builder.Entity<ParentDependent>().HasKey(x => x.Id);
        builder.Entity<ChildDependent>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolverDependent);

        var parents = new List<ParentDependent>
        {
            new() { Id = 1, Children = [] },
            new() { Id = 2, Children = [] },
        };

        var childrenFile = InMemoryDb2File.Create(
            rows:
            [
                new InMemoryDb2File.Row(10, [10, 1]),
                new InMemoryDb2File.Row(11, [11, 1]),
                new InMemoryDb2File.Row(12, [12, 2]),
                new InMemoryDb2File.Row(13, [13, 0]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(ChildDependent) => (childrenFile, SchemaResolverDependent(tableName)),
                _ => (new EmptyDb2File(), SchemaResolverDependent(tableName)),
            };

        Db2IncludeChainExecutor.Apply<ParentDependent, RowHandle>(
            source: parents,
            model: model,
            tableResolver: TableResolver,
            members: [typeof(ParentDependent).GetProperty(nameof(ParentDependent.Children))!],
            entityFactory: new ReflectionDb2EntityFactory())
            .ShouldNotBeNull();

        parents[0].Children.Select(c => c.Id).ToArray().ShouldBe([10, 11]);
        parents[1].Children.Select(c => c.Id).ToArray().ShouldBe([12]);
    }

    private static Db2TableSchema SchemaResolverFkArray(string tableName)
    {
        return tableName switch
        {
            nameof(ParentFkArray) => new Db2TableSchema(
                tableName: nameof(ParentFkArray),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(ParentFkArray.ChildIds), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 3, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(ChildFkArray)),
                ]),

            nameof(ChildFkArray) => new Db2TableSchema(
                tableName: nameof(ChildFkArray),
                layoutHash: 0,
                physicalColumnCount: 1,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                ]),

            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private static Db2TableSchema SchemaResolverStringKeys(string tableName)
        => tableName switch
        {
            nameof(ParentStringKeys) => new Db2TableSchema(
                tableName: nameof(ParentStringKeys),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(ParentStringKeys.ChildIds), Db2ValueType.String, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(ChildStringKeys)),
                ]),

            nameof(ChildStringKeys) => new Db2TableSchema(
                tableName: nameof(ChildStringKeys),
                layoutHash: 0,
                physicalColumnCount: 1,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                ]),

            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };

    private static Db2TableSchema SchemaResolverDependent(string tableName)
        => tableName switch
        {
            nameof(ParentDependent) => new Db2TableSchema(
                tableName: nameof(ParentDependent),
                layoutHash: 0,
                physicalColumnCount: 1,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                ]),

            nameof(ChildDependent) => new Db2TableSchema(
                tableName: nameof(ChildDependent),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(ChildDependent.ParentId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(ParentDependent)),
                ]),

            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };


    private enum Key
    {
        None = 0,
        One = 1,
        Two = 2,
    }

    private sealed class ParentFkArray
    {
        public int Id { get; set; }

        public Key[] ChildIds { get; set; } = [];

        public ICollection<ChildFkArray> Children { get; set; } = [];
    }

    private sealed class ChildFkArray
    {
        public int Id { get; set; }
    }

    private sealed class ParentStringKeys
    {
        public int Id { get; set; }

        public string ChildIds { get; set; } = string.Empty;

        public ICollection<ChildStringKeys> Children { get; set; } = [];
    }

    private sealed class ChildStringKeys
    {
        public int Id { get; set; }
    }

    private sealed class ParentDependent
    {
        public int Id { get; set; }

        public ICollection<ChildDependent> Children { get; set; } = [];
    }

    private sealed class ChildDependent
    {
        public int Id { get; set; }

        public int ParentId { get; set; }
    }


    private sealed class EmptyDb2File : IDb2File<RowHandle>
    {
        public IDb2FileHeader Header => throw new NotSupportedException();

        public Type RowType => typeof(RowHandle);

        public Db2Flags Flags => default;

        public int RecordsCount => 0;

        public ReadOnlyMemory<byte> DenseStringTableBytes => ReadOnlyMemory<byte>.Empty;

        public IEnumerable<RowHandle> EnumerateRows() => [];

        public IEnumerable<RowHandle> EnumerateRowHandles() => [];

        public T ReadField<T>(RowHandle handle, int fieldIndex) => throw new NotSupportedException();

        public bool TryGetRowHandle<TId>(TId id, out RowHandle handle)
            where TId : IEquatable<TId>, IComparable<TId>
        {
            handle = default;
            return false;
        }

        public bool TryGetRowById<TId>(TId id, out RowHandle row)
            where TId : IEquatable<TId>, IComparable<TId>
        {
            row = default;
            return false;
        }
    }

    private sealed class InMemoryDb2File(IReadOnlyDictionary<int, object[]> valuesByRowId) : IDb2File<RowHandle>
    {
        public static InMemoryDb2File Create(params Row[] rows)
            => new(rows.ToDictionary(r => r.RowId, r => r.Values));

        public static InMemoryDb2File Create(IEnumerable<Row> rows)
            => new(rows.ToDictionary(r => r.RowId, r => r.Values));

        public readonly record struct Row(int RowId, object[] Values);

        public IDb2FileHeader Header => throw new NotSupportedException();

        public Type RowType => typeof(RowHandle);

        public Db2Flags Flags => default;

        public int RecordsCount => valuesByRowId.Count;

        public ReadOnlyMemory<byte> DenseStringTableBytes => ReadOnlyMemory<byte>.Empty;

        public IEnumerable<RowHandle> EnumerateRows()
            => valuesByRowId.Keys.OrderBy(k => k).Select((id, i) => new RowHandle(0, i, id));

        public IEnumerable<RowHandle> EnumerateRowHandles() => EnumerateRows();

        public T ReadField<T>(RowHandle handle, int fieldIndex)
        {
            var values = valuesByRowId[handle.RowId];
            return (T)values[fieldIndex];
        }

        public bool TryGetRowHandle<TId>(TId id, out RowHandle handle)
            where TId : IEquatable<TId>, IComparable<TId>
        {
            if (id is not int rowId)
                throw new NotSupportedException($"Only int IDs are supported by {nameof(InMemoryDb2File)}.");

            if (valuesByRowId.ContainsKey(rowId))
            {
                handle = new RowHandle(0, 0, rowId);
                return true;
            }

            handle = default;
            return false;
        }

        public bool TryGetRowById<TId>(TId id, out RowHandle row)
            where TId : IEquatable<TId>, IComparable<TId>
        {
            if (TryGetRowHandle(id, out var handle))
            {
                row = handle;
                return true;
            }

            row = default;
            return false;
        }
    }
}
