using System.Linq.Expressions;
using System.Text;

using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.Formats;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2RowPredicateCompilerTests
{
    [Fact]
    public void TryCompile_returns_false_for_naked_entity_parameter()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, bool>> predicate = e => e != null;

        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, predicate, out _)
            .ShouldBeFalse();
    }

    [Fact]
    public void TryCompile_returns_false_for_field_member_access()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, bool>> predicate = e => e.LevelField > 0;

        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, predicate, out _)
            .ShouldBeFalse();
    }

    [Fact]
    public void TryCompile_returns_false_when_member_mapping_cannot_be_resolved()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, bool>> predicate = e => e.Missing > 0;

        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, predicate, out _)
            .ShouldBeFalse();
    }

    [Fact]
    public void TryCompile_compiles_scalar_predicate_and_captures_requirements()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, bool>> predicate = e => e.Level > 0;

        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, predicate, out var rowPredicate, out var requirements)
            .ShouldBeTrue();

        file.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([1]);

        requirements.Columns.ShouldContain(c => c.Field.Name.Equals(nameof(Entity.Level), StringComparison.OrdinalIgnoreCase) && c.Kind == Db2RequiredColumnKind.Scalar);
    }

    [Fact]
    public void TryCompile_compiles_convert_on_property_access()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, bool>> predicate = e => (long)e.Level > 0;

        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, predicate, out var rowPredicate, out var requirements)
            .ShouldBeTrue();

        file.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([1]);

        requirements.Columns.ShouldContain(c => c.Field.Name.Equals(nameof(Entity.Level), StringComparison.OrdinalIgnoreCase) && c.Kind == Db2RequiredColumnKind.Scalar);
    }

    [Fact]
    public void TryCompile_compiles_string_contains_without_dense_optimization()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, bool>> predicate = e => e.Name.Contains("ph");

        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, predicate, out var rowPredicate, out var requirements)
            .ShouldBeTrue();

        file.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([1]);

        requirements.Columns.ShouldContain(c => c.Field.Name.Equals(nameof(Entity.Name), StringComparison.OrdinalIgnoreCase) && c.Kind == Db2RequiredColumnKind.String);
    }

    [Fact]
    public void TryCompile_compiles_string_contains_with_char_needle_without_dense_optimization()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, bool>> predicate = e => e.Name.Contains('a');

        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, predicate, out var rowPredicate, out _)
            .ShouldBeTrue();

        file.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([1, 2]);
    }

    [Fact]
    public void TryCompile_compiles_string_predicates_with_dense_optimization()
    {
        var (bytes, offsets) = BuildDenseStringTable(["alpha", "beta", "gamma"]);

        var model = BuildModel(SchemaResolver);
        var entityType = model.GetEntityType(typeof(Entity));

        var file = DenseStringInMemoryDb2File.Create(
            tableName: nameof(Entity),
            flags: Db2Flags.None,
            denseStringTableBytes: bytes,
            denseStringIndexProvider: offsets,
            rows:
            [
                new DenseStringInMemoryDb2File.Row(1, [1, 1, "alpha"]),
                new DenseStringInMemoryDb2File.Row(2, [2, -1, "beta"]),
                new DenseStringInMemoryDb2File.Row(3, [3, 0, "gamma"]),
            ]);

        Expression<Func<Entity, bool>> contains = e => e.Name.Contains("ph");
        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, contains, out var containsPredicate)
            .ShouldBeTrue();

        file.EnumerateRows().Where(containsPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([1]);

        Expression<Func<Entity, bool>> starts = e => e.Name.StartsWith("ga");
        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, starts, out var startsPredicate)
            .ShouldBeTrue();

        file.EnumerateRows().Where(startsPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([3]);

        Expression<Func<Entity, bool>> ends = e => e.Name.EndsWith("ta");
        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, ends, out var endsPredicate)
            .ShouldBeTrue();

        file.EnumerateRows().Where(endsPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([2]);
    }

    [Fact]
    public void TryCompile_returns_false_for_string_predicates_with_more_than_one_argument()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, bool>> predicate = e => e.Name.Contains('a', StringComparison.Ordinal);

        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, predicate, out _)
            .ShouldBeFalse();
    }

    [Fact]
    public void TryCompile_returns_false_for_string_predicates_with_non_constant_needle()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, bool>> predicate = e => e.Name.Contains(e.Name);

        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, predicate, out _)
            .ShouldBeFalse();
    }

    [Fact]
    public void TryCompile_returns_false_for_string_materialization_of_virtual_fields()
    {
        var model = BuildModel(SchemaResolverVirtualString);
        var entityType = model.GetEntityType(typeof(Entity));

        var file = InMemoryDb2File.Create(
            tableName: nameof(Entity),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "alpha"]),
            ]);

        Expression<Func<Entity, bool>> direct = e => e.Name == "alpha";
        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, direct, out _)
            .ShouldBeFalse();

        Expression<Func<Entity, bool>> contains = e => e.Name.Contains('a');
        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, contains, out _)
            .ShouldBeFalse();

        Expression<Func<Entity, bool>> convert = e => ((object)e.Name) != null;
        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, convert, out _)
            .ShouldBeFalse();
    }

    [Fact]
    public void TryCompile_can_project_schema_array_collection_reads_in_predicates()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, bool>> predicate = e => e.Ints == null;

        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, predicate, out var rowPredicate, out var requirements)
            .ShouldBeTrue();

        file.EnumerateRows().Where(rowPredicate).ToArray().ShouldBeEmpty();

        requirements.Columns.ShouldContain(c => c.Field.Name.Equals(nameof(Entity.Ints), StringComparison.OrdinalIgnoreCase) && c.Kind == Db2RequiredColumnKind.Scalar);
    }

    [Fact]
    public void TryCompile_does_not_use_array_mapping_for_unsupported_collection_targets()
    {
        var (entityType, file) = CreateFixture();

        Expression<Func<Entity, bool>> arrayTarget = e => e.IntArray == null;
        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, arrayTarget, out var arrayPredicate)
            .ShouldBeTrue();

        file.EnumerateRows().Where(arrayPredicate).ToArray().ShouldBeEmpty();

        Expression<Func<Entity, bool>> setTarget = e => e.IntSet == null;
        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, setTarget, out var setPredicate)
            .ShouldBeTrue();

        file.EnumerateRows().Where(setPredicate).ToArray().ShouldBeEmpty();

        Expression<Func<Entity, bool>> strings = e => e.Strings == null;
        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, strings, out var stringsPredicate)
            .ShouldBeTrue();

        file.EnumerateRows().Where(stringsPredicate).ToArray().ShouldBeEmpty();

        Expression<Func<Entity, bool>> dates = e => e.Dates == null;
        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, dates, out var datesPredicate)
            .ShouldBeTrue();

        file.EnumerateRows().Where(datesPredicate).ToArray().ShouldBeEmpty();

        Expression<Func<Entity, bool>> one = e => e.OneIntList == null;
        Db2RowPredicateCompiler.TryCompile<Entity, RowHandle>(file, entityType, one, out var onePredicate)
            .ShouldBeTrue();

        file.EnumerateRows().Where(onePredicate).ToArray().ShouldBeEmpty();
    }

    private static (Db2EntityType EntityType, InMemoryDb2File File) CreateFixture()
    {
        var model = BuildModel(SchemaResolver);
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
                    1,
                    "alpha",
                    new[] { 1, 2, 3 },
                    new[] { 4, 5, 6 },
                    new HashSet<int> { 7, 8, 9 },
                    new[] { "a", "b", "c" },
                    new[] { new DateTime(2020, 1, 1), new DateTime(2020, 1, 2) },
                    new List<int> { 99 },
                ]),
                new InMemoryDb2File.Row(2,
                [
                    2,
                    -1,
                    "beta",
                    new[] { 10, 11, 12 },
                    new[] { 13, 14, 15 },
                    new HashSet<int> { 16, 17, 18 },
                    new[] { "x", "y", "z" },
                    new[] { new DateTime(2020, 2, 1), new DateTime(2020, 2, 2) },
                    new List<int> { 100 },
                ]),
            ]);

        return (entityType, file);
    }

    private static Db2Model BuildModel(Func<string, Db2TableSchema> resolver)
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<Entity>().HasKey(x => x.Id);
        return builder.Build(resolver);
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

    private static Db2TableSchema SchemaResolverVirtualString(string tableName)
    {
        return tableName switch
        {
            nameof(Entity) => new Db2TableSchema(
                tableName: nameof(Entity),
                layoutHash: 0,
                physicalColumnCount: 3,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Entity.Level), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Entity.Name), Db2ValueType.String, ColumnStartIndex: 2, ElementCount: 1, IsVerified: true, IsVirtual: true, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private static (ReadOnlyMemory<byte> Bytes, IReadOnlyDictionary<string, int> OffsetByString) BuildDenseStringTable(string[] strings)
    {
        var bytes = new List<byte>(capacity: strings.Sum(s => Encoding.UTF8.GetByteCount(s) + 1));
        Dictionary<string, int> offsetByString = new(StringComparer.Ordinal);

        for (var i = 0; i < strings.Length; i++)
        {
            var s = strings[i];
            offsetByString[s] = bytes.Count;
            bytes.AddRange(Encoding.UTF8.GetBytes(s));
            bytes.Add(0);
        }

        return (bytes.ToArray(), offsetByString);
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

    private sealed class DenseStringInMemoryDb2File(string tableName,
        Db2Flags flags,
        ReadOnlyMemory<byte> denseStringTableBytes,
        IReadOnlyDictionary<string, int> denseStringIndexProvider,
        IReadOnlyDictionary<int, object[]> valuesByRowId) : IDb2File<RowHandle>, IDb2DenseStringTableIndexProvider<RowHandle>
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
                throw new NotSupportedException($"Only int IDs are supported by {nameof(DenseStringInMemoryDb2File)}.");

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

        public bool TryGetDenseStringTableIndex(RowHandle row, int fieldIndex, out int stringTableIndex)
        {
            var value = ReadField<string>(row, fieldIndex);
            return denseStringIndexProvider.TryGetValue(value, out stringTableIndex);
        }

        public static DenseStringInMemoryDb2File Create(
            string tableName,
            Db2Flags flags,
            ReadOnlyMemory<byte> denseStringTableBytes,
            IReadOnlyDictionary<string, int> denseStringIndexProvider,
            IReadOnlyList<Row> rows)
        {
            return new DenseStringInMemoryDb2File(
                tableName,
                flags,
                denseStringTableBytes,
                denseStringIndexProvider,
                rows.ToDictionary(r => r.Id, r => r.Values));
        }

        public readonly record struct Row(int Id, object[] Values);
    }
}
