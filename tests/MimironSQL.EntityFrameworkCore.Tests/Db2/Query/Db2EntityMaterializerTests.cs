using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.Formats;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2EntityMaterializerTests
{
    [Fact]
    public void Materialize_sets_scalar_arrays_and_schema_array_collections()
    {
        var (entityType, file) = CreateEntityTypeAndFile();

        var materializer = new Db2EntityMaterializer<MaterializeEntity, RowHandle>(entityType, new ReflectionDb2EntityFactory());

        var entity = materializer.Materialize(file, new RowHandle(0, 0, rowId: 1));

        entity.Id.ShouldBe(123);
        entity.Values.ShouldBe([1, 2, 3]);
        entity.Numbers.ShouldBe([7, 8]);
    }

    [Fact]
    public void Materialize_skips_virtual_strings_and_string_arrays()
    {
        var (entityType, file) = CreateEntityTypeAndFile();

        var materializer = new Db2EntityMaterializer<MaterializeEntity, RowHandle>(entityType, new ReflectionDb2EntityFactory());

        var entity = materializer.Materialize(file, new RowHandle(0, 0, rowId: 1));

        entity.VirtualName.ShouldBe(string.Empty);
        entity.Strings.ShouldBeEmpty();

        // Unsupported element type for schema-array collections should be ignored.
        entity.NotPrimitiveNumbers.ShouldBeNull();
    }

    [Fact]
    public void Materializer_throws_for_non_writable_property()
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<NonWritableEntity>().HasKey(x => x.Id);

        var model = builder.Build(static tableName => tableName switch
        {
            nameof(NonWritableEntity) => new Db2TableSchema(
                tableName: nameof(NonWritableEntity),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(NonWritableEntity.ReadOnlyValue), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        });

        var entityType = model.GetEntityType(typeof(NonWritableEntity));

        var ex = Should.Throw<NotSupportedException>(() =>
            _ = new Db2EntityMaterializer<NonWritableEntity, RowHandle>(entityType, new ReflectionDb2EntityFactory()));

        ex.Message.ShouldContain("must be writable");
    }

    private static (Db2EntityType EntityType, IDb2File<RowHandle> File) CreateEntityTypeAndFile()
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<MaterializeEntity>().HasKey(x => x.Id);

        var model = builder.Build(static tableName => tableName switch
        {
            nameof(MaterializeEntity) => new Db2TableSchema(
                tableName: nameof(MaterializeEntity),
                layoutHash: 0,
                physicalColumnCount: 6,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(MaterializeEntity.Values), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 3, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(MaterializeEntity.Numbers), Db2ValueType.Int64, ColumnStartIndex: 2, ElementCount: 2, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(MaterializeEntity.VirtualName), Db2ValueType.String, ColumnStartIndex: 3, ElementCount: 1, IsVerified: true, IsVirtual: true, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(MaterializeEntity.Strings), Db2ValueType.String, ColumnStartIndex: 4, ElementCount: 2, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(MaterializeEntity.NotPrimitiveNumbers), Db2ValueType.Int64, ColumnStartIndex: 5, ElementCount: 2, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        });

        var entityType = model.GetEntityType(typeof(MaterializeEntity));

        var file = new SingleRowDb2File(
            rowId: 1,
            values:
            [
                123, // Id
                new[] { 1, 2, 3 }, // Values
                new[] { 7, 8 }, // Numbers
                "SHOULD_NOT_BE_READ", // VirtualName (skipped)
                new[] { "a", "b" }, // Strings (skipped due to string[])
                new[] { 1, 2 }, // NotPrimitiveNumbers (skipped due to element type)
            ]);

        return (entityType, file);
    }

    private sealed class MaterializeEntity
    {
        public int Id { get; set; }

        public int[] Values { get; set; } = [];

        public ICollection<int> Numbers { get; set; } = [];

        public string VirtualName { get; set; } = string.Empty;

        public string[] Strings { get; set; } = [];

        public ICollection<decimal>? NotPrimitiveNumbers { get; set; }
    }

    private sealed class NonWritableEntity
    {
        public int Id { get; set; }

        public int ReadOnlyValue { get; } = 0;
    }

    private sealed class SingleRowDb2File(int rowId, object[] values) : IDb2File<RowHandle>
    {
        public IDb2FileHeader Header => null!;

        public Type RowType => typeof(RowHandle);

        public Db2Flags Flags => default;

        public int RecordsCount => 1;

        public ReadOnlyMemory<byte> DenseStringTableBytes => ReadOnlyMemory<byte>.Empty;

        public IEnumerable<RowHandle> EnumerateRowHandles() => EnumerateRows();

        public IEnumerable<RowHandle> EnumerateRows() => [new RowHandle(0, 0, rowId)];

        public T ReadField<T>(RowHandle handle, int fieldIndex) => (T)values[fieldIndex];

        public bool TryGetRowHandle<TId>(TId id, out RowHandle handle) where TId : IEquatable<TId>, IComparable<TId>
        {
            if (id is int i && i == rowId)
            {
                handle = new RowHandle(0, 0, rowId);
                return true;
            }

            handle = default;
            return false;
        }

        public bool TryGetRowById<TId>(TId id, out RowHandle row) where TId : IEquatable<TId>, IComparable<TId>
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
