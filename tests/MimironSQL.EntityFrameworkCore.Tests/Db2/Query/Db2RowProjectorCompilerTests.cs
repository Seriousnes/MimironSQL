using System.Linq.Expressions;

using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.Formats;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2RowProjectorCompilerTests
{
    [Fact]
    public void TryCompile_returns_false_when_file_is_null()
    {
        var entityType = BuildModel().GetEntityType(typeof(Entity));

        Expression<Func<Entity, int>> selector = e => e.Level;

        Db2RowProjectorCompiler.TryCompile<Entity, int, RowHandle>(entityType, selector, out _)
            .ShouldBeFalse();
    }

    [Fact]
    public void TryCompile_returns_false_for_naked_entity_parameter()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, Entity>> selector = e => e;

        Db2RowProjectorCompiler.TryCompile<Entity, Entity, RowHandle>(file, entityType, selector, out _, out _)
            .ShouldBeFalse();
    }

    [Fact]
    public void TryCompile_returns_false_for_field_member_access()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, int>> selector = e => e.LevelField;

        Db2RowProjectorCompiler.TryCompile<Entity, int, RowHandle>(file, entityType, selector, out _, out _)
            .ShouldBeFalse();
    }

    [Fact]
    public void TryCompile_compiles_scalar_property_projection_and_captures_requirements()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, int>> selector = e => e.Level;

        Db2RowProjectorCompiler.TryCompile<Entity, int, RowHandle>(file, entityType, selector, out var projector, out var requirements)
            .ShouldBeTrue();

        projector(new RowHandle(0, 0, 1)).ShouldBe(123);

        requirements.Columns.ShouldContain(new Db2RequiredColumn(entityType.ResolveFieldSchema(typeof(Entity).GetProperty(nameof(Entity.Level))!, "assert"), Db2RequiredColumnKind.Scalar));
    }

    [Fact]
    public void TryCompile_compiles_string_property_projection_and_captures_requirements()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, string>> selector = e => e.Name;

        Db2RowProjectorCompiler.TryCompile<Entity, string, RowHandle>(file, entityType, selector, out var projector, out var requirements)
            .ShouldBeTrue();

        projector(new RowHandle(0, 0, 1)).ShouldBe("alpha");

        requirements.Columns.ShouldContain(new Db2RequiredColumn(entityType.ResolveFieldSchema(typeof(Entity).GetProperty(nameof(Entity.Name))!, "assert"), Db2RequiredColumnKind.String));
    }

    [Fact]
    public void TryCompile_compiles_convert_on_property_access()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, long>> selector = e => e.Level;

        Db2RowProjectorCompiler.TryCompile<Entity, long, RowHandle>(file, entityType, selector, out var projector, out _)
            .ShouldBeTrue();

        projector(new RowHandle(0, 0, 1)).ShouldBe(123L);
    }

    [Fact]
    public void TryCompile_compiles_schema_array_collection_projection_and_returns_array_instance()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, IReadOnlyList<int>>> selector = e => e.Ints;

        Db2RowProjectorCompiler.TryCompile<Entity, IReadOnlyList<int>, RowHandle>(file, entityType, selector, out var projector, out _)
            .ShouldBeTrue();

        var result = projector(new RowHandle(0, 0, 1));
        result.ShouldBeOfType<int[]>();
        ((int[])result).ShouldBe([1, 2, 3]);
    }

    [Fact]
    public void TryCompile_does_not_use_array_mapping_when_element_count_is_one()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, IReadOnlyList<int>>> selector = e => e.OneIntList;

        Db2RowProjectorCompiler.TryCompile<Entity, IReadOnlyList<int>, RowHandle>(file, entityType, selector, out var projector, out _)
            .ShouldBeTrue();

        projector(new RowHandle(0, 0, 1)).ShouldBe([99]);
    }

    [Fact]
    public void TryCompile_does_not_use_array_mapping_when_target_type_is_not_generic()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, int[]>> selector = e => e.IntArray;

        Db2RowProjectorCompiler.TryCompile<Entity, int[], RowHandle>(file, entityType, selector, out var projector, out _)
            .ShouldBeTrue();

        projector(new RowHandle(0, 0, 1)).ShouldBe([4, 5, 6]);
    }

    [Fact]
    public void TryCompile_does_not_use_array_mapping_when_generic_definition_is_not_supported()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, HashSet<int>>> selector = e => e.IntSet;

        Db2RowProjectorCompiler.TryCompile<Entity, HashSet<int>, RowHandle>(file, entityType, selector, out var projector, out _)
            .ShouldBeTrue();

        projector(new RowHandle(0, 0, 1)).OrderBy(x => x).ToArray().ShouldBe([7, 8, 9]);
    }

    [Fact]
    public void TryCompile_does_not_use_array_mapping_when_element_type_is_string()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, IReadOnlyList<string>>> selector = e => e.Strings;

        Db2RowProjectorCompiler.TryCompile<Entity, IReadOnlyList<string>, RowHandle>(file, entityType, selector, out var projector, out _)
            .ShouldBeTrue();

        var result = projector(new RowHandle(0, 0, 1));
        result.ShouldBeOfType<string[]>();
        ((string[])result).ShouldBe(["a", "b", "c"]);
    }

    [Fact]
    public void TryCompile_does_not_use_array_mapping_when_element_type_is_not_primitive()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, IReadOnlyList<DateTime>>> selector = e => e.Dates;

        Db2RowProjectorCompiler.TryCompile<Entity, IReadOnlyList<DateTime>, RowHandle>(file, entityType, selector, out var projector, out _)
            .ShouldBeTrue();

        var result = projector(new RowHandle(0, 0, 1));
        result.ShouldBeOfType<DateTime[]>();
        ((DateTime[])result).Length.ShouldBe(2);
    }

    [Fact]
    public void TryCompile_returns_false_when_member_mapping_cannot_be_resolved()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, int>> selector = e => e.Missing;

        Db2RowProjectorCompiler.TryCompile<Entity, int, RowHandle>(file, entityType, selector, out _, out _)
            .ShouldBeFalse();
    }

    [Fact]
    public void TryCompile_returns_false_for_binary_expressions()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, int>> selector = e => e.Level + 1;

        Db2RowProjectorCompiler.TryCompile<Entity, int, RowHandle>(file, entityType, selector, out _, out _)
            .ShouldBeFalse();
    }

    [Fact]
    public void TryCompile_returns_false_for_method_calls()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, string>> selector = e => e.Name.ToUpper();

        Db2RowProjectorCompiler.TryCompile<Entity, string, RowHandle>(file, entityType, selector, out _, out _)
            .ShouldBeFalse();
    }

    [Fact]
    public void TryCompile_returns_false_for_conditional_expressions()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, int>> selector = e => e.Level > 0 ? e.Level : 0;

        Db2RowProjectorCompiler.TryCompile<Entity, int, RowHandle>(file, entityType, selector, out _, out _)
            .ShouldBeFalse();
    }

    [Fact]
    public void TryCompile_returns_false_for_new_array_expressions()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, int[]>> selector = e => new[] { e.Level };

        Db2RowProjectorCompiler.TryCompile<Entity, int[], RowHandle>(file, entityType, selector, out _, out _)
            .ShouldBeFalse();
    }

    private static (Db2EntityType EntityType, InMemoryDb2File File) CreateFixture()
    {
        var model = BuildModel();
        var entityType = model.GetEntityType(typeof(Entity));

        var file = InMemoryDb2File.Create(
            tableName: nameof(Entity),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1,
                [
                    1,
                    123,
                    "alpha",
                    new[] { 1, 2, 3 },
                    new[] { 4, 5, 6 },
                    new HashSet<int> { 7, 8, 9 },
                    new[] { "a", "b", "c" },
                    new[] { new DateTime(2020, 1, 1), new DateTime(2020, 1, 2) },
                    new List<int> { 99 },
                ]),
            ]);

        return (entityType, file);
    }

    private static Db2Model BuildModel()
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<Entity>().HasKey(x => x.Id);
        return builder.Build(SchemaResolver);
    }

    private static Db2TableSchema SchemaResolver(string tableName)
    {
        return tableName switch
        {
            nameof(Entity) => new Db2TableSchema(
                tableName: nameof(Entity),
                layoutHash: 0,
                physicalColumnCount: 9,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Entity.Level), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Entity.Name), Db2ValueType.String, ColumnStartIndex: 2, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Entity.Ints), Db2ValueType.Int64, ColumnStartIndex: 3, ElementCount: 3, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Entity.IntArray), Db2ValueType.Int64, ColumnStartIndex: 4, ElementCount: 3, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Entity.IntSet), Db2ValueType.Int64, ColumnStartIndex: 5, ElementCount: 3, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Entity.Strings), Db2ValueType.String, ColumnStartIndex: 6, ElementCount: 3, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Entity.Dates), Db2ValueType.Int64, ColumnStartIndex: 7, ElementCount: 2, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Entity.OneIntList), Db2ValueType.Int64, ColumnStartIndex: 8, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private sealed class Entity
    {
        public int Id { get; set; }

        public int Level { get; set; }

        public string Name { get; set; } = string.Empty;

        public IReadOnlyList<int> Ints { get; set; } = [];

        public int[] IntArray { get; set; } = [];

        public HashSet<int> IntSet { get; set; } = [];

        public IReadOnlyList<string> Strings { get; set; } = [];

        public IReadOnlyList<DateTime> Dates { get; set; } = [];

        public IReadOnlyList<int> OneIntList { get; set; } = [];

        public int Missing { get; set; }

        public int LevelField;
    }

    private sealed class InMemoryDb2File(string tableName, Db2Flags flags, ReadOnlyMemory<byte> denseStringTableBytes, IReadOnlyDictionary<int, object[]> valuesByRowId)
        : IDb2File<RowHandle>
    {
        public string TableName { get; } = tableName;

        public Type RowType { get; } = typeof(RowHandle);

        public Db2Flags Flags { get; } = flags;

        public int RecordsCount { get; } = valuesByRowId.Count;

        public ReadOnlyMemory<byte> DenseStringTableBytes { get; } = denseStringTableBytes;

        public IEnumerable<RowHandle> EnumerateRowHandles() => EnumerateRows();

        public IEnumerable<RowHandle> EnumerateRows()
            => valuesByRowId.Keys.OrderBy(k => k).Select((id, i) => new RowHandle(0, i, id));

        public T ReadField<T>(RowHandle handle, int fieldIndex)
        {
            var values = valuesByRowId[handle.RowId];
            return (T)values[fieldIndex];
        }

        public bool TryGetRowHandle<TId>(TId id, out RowHandle handle) where TId : IEquatable<TId>, IComparable<TId>
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

        public static InMemoryDb2File Create(string tableName, Db2Flags flags, ReadOnlyMemory<byte> denseStringTableBytes, IReadOnlyList<Row> rows)
        {
            return new InMemoryDb2File(
                tableName,
                flags,
                denseStringTableBytes,
                rows.ToDictionary(r => r.Id, r => r.Values));
        }

        public readonly record struct Row(int Id, object[] Values);
    }
}
