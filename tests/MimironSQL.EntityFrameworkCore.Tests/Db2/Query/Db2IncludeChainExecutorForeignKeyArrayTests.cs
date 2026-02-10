using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.Formats;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2IncludeChainExecutorForeignKeyArrayTests
{
    [Fact]
    public void Apply_supports_foreign_key_array_include_with_enum_array_keys()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<Parent>()
            .HasMany(x => x.Children)
            .WithForeignKeyArray<Key>(x => x.ChildIds);

        builder.Entity<Parent>().HasKey(x => x.Id);
        builder.Entity<Child>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolver);

        var parent = new Parent
        {
            Id = 1,
            ChildIds = [Key.None, Key.None],
            Children = [],
        };

        var result = Db2IncludeChainExecutor.Apply<Parent, RowHandle>(
            source: [parent],
            model: model,
            tableResolver: TableResolver,
            members: [typeof(Parent).GetProperty(nameof(Parent.Children))!],
            entityFactory: new ReflectionDb2EntityFactory());

        result.ShouldNotBeNull();
        parent.Children.ShouldNotBeNull();
        parent.Children.Count.ShouldBe(0);
    }

    private static (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
        => (new EmptyDb2File(), SchemaResolver(tableName));

    private static Db2TableSchema SchemaResolver(string tableName)
    {
        return tableName switch
        {
            nameof(Parent) => new Db2TableSchema(
                tableName: nameof(Parent),
                layoutHash: 0,
                physicalColumnCount: 3,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.ChildIds), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 2, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(Child)),
                    new Db2FieldSchema(nameof(Parent.Children), Db2ValueType.Int64, ColumnStartIndex: 2, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(Child)),
                ]),

            nameof(Child) => new Db2TableSchema(
                tableName: nameof(Child),
                layoutHash: 0,
                physicalColumnCount: 1,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                ]),

            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private enum Key
    {
        None = 0,
        One = 1,
    }

    private sealed class Parent
    {
        public int Id { get; set; }

        public Key[] ChildIds { get; set; } = [];

        public ICollection<Child> Children { get; set; } = [];
    }

    private sealed class Child
    {
        public int Id { get; set; }
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

        public bool TryGetRowHandle<TId>(TId id, out RowHandle handle) where TId : IEquatable<TId>, IComparable<TId>
        {
            handle = default;
            return false;
        }

        public bool TryGetRowById<TId>(TId id, out RowHandle row) where TId : IEquatable<TId>, IComparable<TId>
        {
            row = default;
            return false;
        }
    }
}
