using System.Linq.Expressions;

using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.Formats;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2BatchedNavigationProjectorTests
{
    [Fact]
    public void Project_can_project_root_and_navigation_members()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<Root>()
            .HasOne(x => x.Target)
            .WithForeignKey(x => x.TargetId);

        builder.Entity<Root>().HasKey(x => x.Id);
        builder.Entity<Target>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolver);

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

        var source = new[]
        {
            new Root { Id = 1, TargetId = 10, Name = "r1" },
            new Root { Id = 2, TargetId = 0, Name = "r2" },
        };

        var results = Db2BatchedNavigationProjector.Project<Root, Projected, RowHandle>(
                source,
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
    public void Project_can_materialize_array_fields_as_generic_collection_interfaces()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<RootList>()
            .HasOne(x => x.Target)
            .WithForeignKey(x => x.TargetId);

        builder.Entity<RootList>().HasKey(x => x.Id);
        builder.Entity<TargetList>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolverList);

        var targetFile = InMemoryDb2File.Create(
            tableName: nameof(TargetList),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(10, [10, new[] { 1, 2, 3 }]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(TargetList) => (targetFile, SchemaResolverList(nameof(TargetList))),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<RootList, IReadOnlyList<int>?>> selector = x => x.Target!.Values;
        var accesses = Db2NavigationQueryTranslator.GetNavigationAccesses<RootList>(model, selector);

        var results = Db2BatchedNavigationProjector.Project<RootList, IReadOnlyList<int>?, RowHandle>(
                [new RootList { Id = 1, TargetId = 10 }],
                model,
                TableResolver,
                [.. accesses],
                selector,
                take: null)
            .ToArray();

        results.Single().ShouldNotBeNull()!.ToArray().ShouldBe([1, 2, 3]);
    }

    [Fact]
    public void Project_applies_take()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<Root>()
            .HasOne(x => x.Target)
            .WithForeignKey(x => x.TargetId);

        builder.Entity<Root>().HasKey(x => x.Id);
        builder.Entity<Target>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolver);

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
                nameof(Target) => (targetFile, SchemaResolver(nameof(Target))),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Root, string?>> selector = x => x.Target!.Name;
        var accesses = Db2NavigationQueryTranslator.GetNavigationAccesses<Root>(model, selector);

        var source = new[]
        {
            new Root { Id = 1, TargetId = 10, Name = "r1" },
            new Root { Id = 2, TargetId = 10, Name = "r2" },
        };

        var results = Db2BatchedNavigationProjector.Project<Root, string?, RowHandle>(
                source,
                model,
                TableResolver,
                [.. accesses],
                selector,
                take: 1)
            .ToArray();

        results.ShouldBe(["t10"]);
    }

    [Fact]
    public void Project_throws_when_materializing_virtual_string_field_as_string()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<RootVirtualString>()
            .HasOne(x => x.Target)
            .WithForeignKey(x => x.TargetId);

        builder.Entity<RootVirtualString>().HasKey(x => x.Id);
        builder.Entity<TargetVirtualString>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolverVirtualString);

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
                nameof(TargetVirtualString) => (targetFile, SchemaResolverVirtualString(nameof(TargetVirtualString))),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<RootVirtualString, string>> selector = x => x.Target!.VirtualName;
        var accesses = Db2NavigationQueryTranslator.GetNavigationAccesses<RootVirtualString>(model, selector);

        Should.Throw<NotSupportedException>(() =>
        {
            _ = Db2BatchedNavigationProjector.Project<RootVirtualString, string, RowHandle>(
                    [new RootVirtualString { Id = 1, TargetId = 10 }],
                    model,
                    TableResolver,
                    [.. accesses],
                    selector,
                    take: null)
                .ToArray();
        });
    }

    [Fact]
    public void Project_throws_when_string_array_is_materialized_as_generic_collection_interface()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<RootStringArrayList>()
            .HasOne(x => x.Target)
            .WithForeignKey(x => x.TargetId);

        builder.Entity<RootStringArrayList>().HasKey(x => x.Id);
        builder.Entity<TargetStringArrayList>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolverStringArrayList);

        var targetFile = InMemoryDb2File.Create(
            tableName: nameof(TargetStringArrayList),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(10, [10, new[] { "a", "b" }]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(TargetStringArrayList) => (targetFile, SchemaResolverStringArrayList(nameof(TargetStringArrayList))),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<RootStringArrayList, IReadOnlyList<string>>> selector = x => x.Target!.Names;
        var accesses = Db2NavigationQueryTranslator.GetNavigationAccesses<RootStringArrayList>(model, selector);

        Should.Throw<NotSupportedException>(() =>
        {
            _ = Db2BatchedNavigationProjector.Project<RootStringArrayList, IReadOnlyList<string>, RowHandle>(
                    [new RootStringArrayList { Id = 1, TargetId = 10 }],
                    model,
                    TableResolver,
                    [.. accesses],
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

    private static Db2TableSchema SchemaResolverList(string tableName)
        => tableName switch
        {
            nameof(RootList) => new Db2TableSchema(
                tableName: nameof(RootList),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(RootList.TargetId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(TargetList)),
                ]),

            nameof(TargetList) => new Db2TableSchema(
                tableName: nameof(TargetList),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(TargetList.Values), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 3, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
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
                    new Db2FieldSchema(nameof(RootStringArrayList.TargetId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(TargetStringArrayList)),
                ]),

            nameof(TargetStringArrayList) => new Db2TableSchema(
                tableName: nameof(TargetStringArrayList),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(TargetStringArrayList.Names), Db2ValueType.String, ColumnStartIndex: 1, ElementCount: 2, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
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

    private sealed class RootList
    {
        public int Id { get; set; }
        public int TargetId { get; set; }
        public TargetList? Target { get; set; }
    }

    private sealed class TargetList
    {
        public int Id { get; set; }
        public IReadOnlyList<int> Values { get; set; } = [];
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
        public int TargetId { get; set; }
        public TargetStringArrayList? Target { get; set; }
    }

    private sealed class TargetStringArrayList
    {
        public int Id { get; set; }
        public IReadOnlyList<string> Names { get; set; } = [];
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
