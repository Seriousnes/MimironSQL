using System.Linq.Expressions;

using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.Formats;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2NavigationRowProjectorTests
{
    [Fact]
    public void ProjectFromRows_can_project_root_and_navigation_members_including_arrays()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<Root>()
            .HasOne(x => x.Target)
            .WithForeignKey(x => x.TargetId);

        builder.Entity<Root>().HasKey(x => x.Id);
        builder.Entity<Target>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolver);

        var rootFile = InMemoryDb2File.Create(
            tableName: nameof(Root),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 10, "r1"]),
                new InMemoryDb2File.Row(2, [2, 0, "r2"]),
            ]);

        var targetFile = InMemoryDb2File.Create(
            tableName: nameof(Target),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(10, [10, "t10", new[] { 1, 2, 3 }]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Root) => (rootFile, SchemaResolver(nameof(Root))),
                nameof(Target) => (targetFile, SchemaResolver(nameof(Target))),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Root, Projected>> selector = x => new Projected
        {
            RootName = x.Name,
            TargetName = x.Target!.Name,
            Values = x.Target.Values,
        };

        var accesses = Db2NavigationQueryTranslator.GetNavigationAccesses<Root>(model, selector);
        accesses.Count.ShouldBe(2);

        var results = Db2NavigationRowProjector.ProjectFromRows<Projected, RowHandle>(
                rootFile,
                rootFile.EnumerateRows(),
                model.GetEntityType(typeof(Root)),
                model,
                TableResolver,
                [.. accesses],
                selector,
                take: null)
            .ToArray();

        results.Length.ShouldBe(2);

        results[0].RootName.ShouldBe("r1");
        results[0].TargetName.ShouldBe("t10");
        results[0].Values.ShouldBe([1, 2, 3]);

        results[1].RootName.ShouldBe("r2");
        results[1].TargetName.ShouldBeNull();
        results[1].Values.ShouldBeNull();
    }

    [Fact]
    public void ProjectFromRows_throws_when_materializing_virtual_string_field_as_string()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<RootVirtualString>()
            .HasOne(x => x.Target)
            .WithForeignKey(x => x.TargetId);

        builder.Entity<RootVirtualString>().HasKey(x => x.Id);
        builder.Entity<TargetVirtualString>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolverVirtualString);

        var rootFile = InMemoryDb2File.Create(
            tableName: nameof(RootVirtualString),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 10]),
            ]);

        var targetFile = InMemoryDb2File.Create(
            tableName: nameof(TargetVirtualString),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(10, [10, "virtual"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(RootVirtualString) => (rootFile, SchemaResolverVirtualString(nameof(RootVirtualString))),
                nameof(TargetVirtualString) => (targetFile, SchemaResolverVirtualString(nameof(TargetVirtualString))),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<RootVirtualString, string>> selector = x => x.Target!.VirtualName;

        var accesses = Db2NavigationQueryTranslator.GetNavigationAccesses<RootVirtualString>(model, selector);
        accesses.Count.ShouldBe(1);

        Should.Throw<NotSupportedException>(() =>
        {
            _ = Db2NavigationRowProjector.ProjectFromRows<string, RowHandle>(
                    rootFile,
                    rootFile.EnumerateRows(),
                    model.GetEntityType(typeof(RootVirtualString)),
                    model,
                    TableResolver,
                    [.. accesses],
                    selector,
                    take: null)
                .ToArray();
        });
    }

    [Fact]
    public void ProjectFromRows_throws_for_string_arrays_in_generic_enumerables()
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<RootStringArrayList>().HasKey(x => x.Id);
        var model = builder.Build(SchemaResolverStringArrayList);

        var rootFile = InMemoryDb2File.Create(
            tableName: nameof(RootStringArrayList),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, new[] { "a", "b" }]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(RootStringArrayList) => (rootFile, SchemaResolverStringArrayList(nameof(RootStringArrayList))),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<RootStringArrayList, IReadOnlyList<string>>> selector = x => x.Names;

        Should.Throw<NotSupportedException>(() =>
        {
            _ = Db2NavigationRowProjector.ProjectFromRows<IReadOnlyList<string>, RowHandle>(
                    rootFile,
                    rootFile.EnumerateRows(),
                    model.GetEntityType(typeof(RootStringArrayList)),
                    model,
                    TableResolver,
                    accesses: [],
                    selector,
                    take: null)
                .ToArray();
        });
    }

    [Fact]
    public void ProjectFromRows_throws_for_non_primitive_array_element_types()
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<RootUnsupportedArray>().HasKey(x => x.Id);
        var model = builder.Build(SchemaResolverUnsupportedArray);

        var rootFile = InMemoryDb2File.Create(
            tableName: nameof(RootUnsupportedArray),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, new[] { DateTime.UnixEpoch }]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(RootUnsupportedArray) => (rootFile, SchemaResolverUnsupportedArray(nameof(RootUnsupportedArray))),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<RootUnsupportedArray, DateTime[]>> selector = x => x.Times;

        Should.Throw<NotSupportedException>(() =>
        {
            _ = Db2NavigationRowProjector.ProjectFromRows<DateTime[], RowHandle>(
                    rootFile,
                    rootFile.EnumerateRows(),
                    model.GetEntityType(typeof(RootUnsupportedArray)),
                    model,
                    TableResolver,
                    accesses: [],
                    selector,
                    take: null)
                .ToArray();
        });
    }

    private static Db2TableSchema SchemaResolver(string tableName)
        => tableName switch
        {
            nameof(Root) => new Db2TableSchema(
                tableName: nameof(Root),
                layoutHash: 0,
                physicalColumnCount: 3,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Root.TargetId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(Target)),
                    new Db2FieldSchema(nameof(Root.Name), Db2ValueType.String, ColumnStartIndex: 2, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            nameof(Target) => new Db2TableSchema(
                tableName: nameof(Target),
                layoutHash: 0,
                physicalColumnCount: 3,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Target.Name), Db2ValueType.String, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Target.Values), Db2ValueType.Int64, ColumnStartIndex: 2, ElementCount: 3, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };

    private static Db2TableSchema SchemaResolverVirtualString(string tableName)
        => tableName switch
        {
            nameof(RootVirtualString) => new Db2TableSchema(
                tableName: nameof(RootVirtualString),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(RootVirtualString.TargetId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(TargetVirtualString)),
                ]),

            nameof(TargetVirtualString) => new Db2TableSchema(
                tableName: nameof(TargetVirtualString),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(TargetVirtualString.VirtualName), Db2ValueType.String, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: true, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };

    private static Db2TableSchema SchemaResolverStringArrayList(string tableName)
        => tableName switch
        {
            nameof(RootStringArrayList) => new Db2TableSchema(
                tableName: nameof(RootStringArrayList),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(RootStringArrayList.Names), Db2ValueType.String, ColumnStartIndex: 1, ElementCount: 2, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),
            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };

    private static Db2TableSchema SchemaResolverUnsupportedArray(string tableName)
        => tableName switch
        {
            nameof(RootUnsupportedArray) => new Db2TableSchema(
                tableName: nameof(RootUnsupportedArray),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(RootUnsupportedArray.Times), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),
            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };

    private sealed class Root
    {
        public int Id { get; set; }
        public int TargetId { get; set; }
        public string Name { get; set; } = string.Empty;
        public Target? Target { get; set; }
    }

    private sealed class Target
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int[] Values { get; set; } = [];
    }

    private sealed class Projected
    {
        public string RootName { get; set; } = string.Empty;
        public string? TargetName { get; set; }
        public int[]? Values { get; set; }
    }

    private sealed class RootVirtualString
    {
        public int Id { get; set; }
        public int TargetId { get; set; }
        public TargetVirtualString? Target { get; set; }
    }

    private sealed class TargetVirtualString
    {
        public int Id { get; set; }
        public string VirtualName { get; set; } = string.Empty;
    }

    private sealed class RootStringArrayList
    {
        public int Id { get; set; }
        public IReadOnlyList<string> Names { get; set; } = [];
    }

    private sealed class RootUnsupportedArray
    {
        public int Id { get; set; }
        public DateTime[] Times { get; set; } = [];
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

        public bool TryGetRowHandle<TId>(TId id, out RowHandle handle)
            where TId : IEquatable<TId>, IComparable<TId>
        {
            var intId = Convert.ToInt32(id);
            if (valuesByRowId.ContainsKey(intId))
            {
                handle = new RowHandle(0, 0, intId);
                return true;
            }

            handle = default;
            return false;
        }

        public bool TryGetRowById<TId>(TId id, out RowHandle row)
            where TId : IEquatable<TId>, IComparable<TId>
        {
            var intId = Convert.ToInt32(id);
            if (!valuesByRowId.ContainsKey(intId))
            {
                row = default;
                return false;
            }

            // RowHandle.RowIndex is only used for dense string lookups; these tests don't depend on it.
            row = new RowHandle(0, 0, intId);
            return true;
        }

        public static InMemoryDb2File Create(string tableName, Db2Flags flags, ReadOnlyMemory<byte> denseStringTableBytes, Row[] rows)
            => new(tableName, flags, denseStringTableBytes, rows.ToDictionary(r => r.RowId, r => r.Values));

        internal readonly record struct Row(int RowId, object[] Values);
    }
}
