using MimironSQL.Db2;
using MimironSQL.Db2.Model;
using MimironSQL.Db2.Query;
using MimironSQL.Db2.Schema;
using MimironSQL.Formats;

using Shouldly;

namespace MimironSQL.Tests;

public sealed class Db2QueryableAsyncExtensionsTests
{
    [Fact]
    public async Task ToListAsync_returns_all_entities()
    {
        var (table, _) = CreateParentTable();

        var result = await table.ToListAsync();

        result.ShouldNotBeNull();
        result.Count.ShouldBe(3);
        result.Select(x => x.Id).OrderBy(x => x).ToArray().ShouldBe([1, 2, 3]);
    }

    [Fact]
    public async Task ToListAsync_with_where_returns_filtered_entities()
    {
        var (table, _) = CreateParentTable();

        var result = await table.Where(x => x.Level > 0).ToListAsync();

        result.ShouldNotBeNull();
        result.Count.ShouldBe(2);
        result.Select(x => x.Id).OrderBy(x => x).ToArray().ShouldBe([1, 2]);
    }

    [Fact]
    public async Task ToListAsync_honors_cancellation_token()
    {
        var (table, _) = CreateParentTable();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await Should.ThrowAsync<OperationCanceledException>(async () =>
            await table.ToListAsync(cts.Token));
    }

    [Fact]
    public async Task ToArrayAsync_returns_all_entities()
    {
        var (table, _) = CreateParentTable();

        var result = await table.ToArrayAsync();

        result.ShouldNotBeNull();
        result.Length.ShouldBe(3);
        result.Select(x => x.Id).OrderBy(x => x).ToArray().ShouldBe([1, 2, 3]);
    }

    [Fact]
    public async Task ToArrayAsync_honors_cancellation_token()
    {
        var (table, _) = CreateParentTable();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await Should.ThrowAsync<OperationCanceledException>(async () =>
            await table.ToArrayAsync(cts.Token));
    }

    [Fact]
    public async Task FirstAsync_returns_first_entity()
    {
        var (table, _) = CreateParentTable();

        var result = await table.Where(x => x.Id == 1).FirstAsync();

        result.ShouldNotBeNull();
        result.Id.ShouldBe(1);
    }

    [Fact]
    public async Task FirstAsync_with_predicate_returns_first_matching_entity()
    {
        var (table, _) = CreateParentTable();

        var result = await table.FirstAsync(x => x.Level > 0);

        result.ShouldNotBeNull();
        result.Level.ShouldBeGreaterThan(0);
    }

    [Fact]
    public async Task FirstAsync_with_predicate_overload_works()
    {
        var (table, _) = CreateParentTable();

        var result = await table.FirstAsync(x => x.Id == 2);

        result.Id.ShouldBe(2);
    }

    [Fact]
    public async Task FirstAsync_throws_when_no_element()
    {
        var (table, _) = CreateParentTable();

        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await table.Where(x => x.Id == 999).FirstAsync());
    }

    [Fact]
    public async Task FirstAsync_honors_cancellation_token()
    {
        var (table, _) = CreateParentTable();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await Should.ThrowAsync<OperationCanceledException>(async () =>
            await table.FirstAsync(cts.Token));
    }

    [Fact]
    public async Task FirstOrDefaultAsync_returns_first_entity()
    {
        var (table, _) = CreateParentTable();

        var result = await table.Where(x => x.Id == 2).FirstOrDefaultAsync();

        result.ShouldNotBeNull();
        result.Id.ShouldBe(2);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_with_predicate_returns_first_matching_entity()
    {
        var (table, _) = CreateParentTable();

        var result = await table.FirstOrDefaultAsync(x => x.Id == 1);

        result.ShouldNotBeNull();
        result.Id.ShouldBe(1);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_with_predicate_overload_works()
    {
        var (table, _) = CreateParentTable();

        var result = await table.FirstOrDefaultAsync(x => x.Level > 3);

        result.ShouldNotBeNull();
        result.Level.ShouldBe(5);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_returns_null_when_no_element()
    {
        var (table, _) = CreateParentTable();

        var result = await table.Where(x => x.Id == 999).FirstOrDefaultAsync();

        result.ShouldBeNull();
    }

    [Fact]
    public async Task FirstOrDefaultAsync_honors_cancellation_token()
    {
        var (table, _) = CreateParentTable();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await Should.ThrowAsync<OperationCanceledException>(async () =>
            await table.FirstOrDefaultAsync(cts.Token));
    }

    [Fact]
    public async Task SingleAsync_returns_single_entity()
    {
        var (table, _) = CreateParentTable();

        var result = await table.Where(x => x.Id == 1).SingleAsync();

        result.ShouldNotBeNull();
        result.Id.ShouldBe(1);
    }

    [Fact]
    public async Task SingleAsync_with_predicate_returns_single_matching_entity()
    {
        var (table, _) = CreateParentTable();

        var result = await table.Where(x => x.Id == 3).SingleAsync();

        result.ShouldNotBeNull();
        result.Id.ShouldBe(3);
    }

    [Fact]
    public async Task SingleAsync_with_predicate_overload_works()
    {
        var (table, _) = CreateParentTable();

        var result = await table.SingleAsync(x => x.Id == 2);

        result.Id.ShouldBe(2);
    }

    [Fact]
    public async Task SingleAsync_throws_when_no_element()
    {
        var (table, _) = CreateParentTable();

        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await table.Where(x => x.Id == 999).SingleAsync());
    }

    [Fact]
    public async Task SingleAsync_throws_when_multiple_elements()
    {
        var (table, _) = CreateParentTable();

        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await table.Where(x => x.Level >= 0).SingleAsync());
    }

    [Fact]
    public async Task SingleAsync_honors_cancellation_token()
    {
        var (table, _) = CreateParentTable();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await Should.ThrowAsync<OperationCanceledException>(async () =>
            await table.SingleAsync(cts.Token));
    }

    [Fact]
    public async Task SingleOrDefaultAsync_returns_single_entity()
    {
        var (table, _) = CreateParentTable();

        var result = await table.Where(x => x.Id == 3).SingleOrDefaultAsync();

        result.ShouldNotBeNull();
        result.Id.ShouldBe(3);
    }

    [Fact]
    public async Task SingleOrDefaultAsync_with_predicate_returns_single_matching_entity()
    {
        var (table, _) = CreateParentTable();

        var result = await table.Where(x => x.Id == 2).SingleOrDefaultAsync();

        result.ShouldNotBeNull();
        result.Id.ShouldBe(2);
    }

    [Fact]
    public async Task SingleOrDefaultAsync_with_predicate_overload_works()
    {
        var (table, _) = CreateParentTable();

        var result = await table.SingleOrDefaultAsync(x => x.Id == 1);

        result.ShouldNotBeNull();
        result.Id.ShouldBe(1);
    }

    [Fact]
    public async Task SingleOrDefaultAsync_returns_null_when_no_element()
    {
        var (table, _) = CreateParentTable();

        var result = await table.Where(x => x.Id == 999).SingleOrDefaultAsync();

        result.ShouldBeNull();
    }

    [Fact]
    public async Task SingleOrDefaultAsync_throws_when_multiple_elements()
    {
        var (table, _) = CreateParentTable();

        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await table.Where(x => x.Level >= 0).SingleOrDefaultAsync());
    }

    [Fact]
    public async Task SingleOrDefaultAsync_honors_cancellation_token()
    {
        var (table, _) = CreateParentTable();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await Should.ThrowAsync<OperationCanceledException>(async () =>
            await table.SingleOrDefaultAsync(cts.Token));
    }

    [Fact]
    public async Task AnyAsync_returns_true_when_elements_exist()
    {
        var (table, _) = CreateParentTable();

        var result = await table.AnyAsync();

        result.ShouldBeTrue();
    }

    [Fact]
    public async Task AnyAsync_with_predicate_returns_true_when_matching_elements_exist()
    {
        var (table, _) = CreateParentTable();

        var result = await table.AnyAsync(x => x.Level > 0);

        result.ShouldBeTrue();
    }

    [Fact]
    public async Task AnyAsync_with_predicate_overload_works()
    {
        var (table, _) = CreateParentTable();

        var result = await table.AnyAsync(x => x.Id == 3);

        result.ShouldBeTrue();
    }

    [Fact]
    public async Task AnyAsync_returns_false_when_no_elements()
    {
        var (table, _) = CreateParentTable();

        var result = await table.Where(x => x.Id == 999).AnyAsync();

        result.ShouldBeFalse();
    }

    [Fact]
    public async Task AnyAsync_honors_cancellation_token()
    {
        var (table, _) = CreateParentTable();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await Should.ThrowAsync<OperationCanceledException>(async () =>
            await table.AnyAsync(cts.Token));
    }

    [Fact]
    public async Task AllAsync_returns_true_when_all_elements_match()
    {
        var (table, _) = CreateParentTable();

        var result = await table.AllAsync(x => x.Level >= 0);

        result.ShouldBeTrue();
    }

    [Fact]
    public async Task AllAsync_returns_false_when_not_all_elements_match()
    {
        var (table, _) = CreateParentTable();

        var result = await table.AllAsync(x => x.Level > 0);

        result.ShouldBeFalse();
    }

    [Fact]
    public async Task AllAsync_honors_cancellation_token()
    {
        var (table, _) = CreateParentTable();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await Should.ThrowAsync<OperationCanceledException>(async () =>
            await table.AllAsync(x => x.Level >= 0, cts.Token));
    }

    [Fact]
    public async Task CountAsync_returns_total_count()
    {
        var (table, _) = CreateParentTable();

        var result = await table.CountAsync();

        result.ShouldBe(3);
    }

    [Fact]
    public async Task CountAsync_with_predicate_returns_filtered_count()
    {
        var (table, _) = CreateParentTable();

        var result = await table.Where(x => x.Level > 0).CountAsync();

        result.ShouldBe(2);
    }

    [Fact]
    public async Task CountAsync_with_predicate_overload_works()
    {
        var (table, _) = CreateParentTable();

        var result = await table.CountAsync(x => x.Level > 0);

        result.ShouldBe(2);
    }

    [Fact]
    public async Task CountAsync_honors_cancellation_token()
    {
        var (table, _) = CreateParentTable();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await Should.ThrowAsync<OperationCanceledException>(async () =>
            await table.CountAsync(cts.Token));
    }

    [Fact]
    public async Task AsAsyncEnumerable_yields_all_entities()
    {
        var (table, _) = CreateParentTable();
        var ids = new List<int>();

        await foreach (var entity in table.AsAsyncEnumerable())
        {
            ids.Add(entity.Id);
        }

        ids.Count.ShouldBe(3);
        ids.OrderBy(x => x).ToArray().ShouldBe([1, 2, 3]);
    }

    [Fact]
    public async Task AsAsyncEnumerable_with_where_yields_filtered_entities()
    {
        var (table, _) = CreateParentTable();
        var ids = new List<int>();

        await foreach (var entity in table.Where(x => x.Level > 0).AsAsyncEnumerable())
        {
            ids.Add(entity.Id);
        }

        ids.Count.ShouldBe(2);
        ids.OrderBy(x => x).ToArray().ShouldBe([1, 2]);
    }

    [Fact]
    public async Task AsAsyncEnumerable_honors_cancellation_token()
    {
        var (table, _) = CreateParentTable();
        var cts = new CancellationTokenSource();
        var count = 0;

        await Should.ThrowAsync<OperationCanceledException>(async () =>
        {
            await foreach (var entity in table.AsAsyncEnumerable(cts.Token))
            {
                count++;
                if (count == 2)
                    cts.Cancel();
            }
        });

        count.ShouldBe(2);
    }

    private static (Db2Table<Parent> Table, Db2QueryProvider<Parent, RowHandle> Provider) CreateParentTable()
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<Parent>().HasKey(x => x.Id);
        var model = builder.Build(SchemaResolver);

        var parentsFile = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1]),
                new InMemoryDb2File.Row(2, [2, 5]),
                new InMemoryDb2File.Row(3, [3, 0]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parentsFile, SchemaResolver(nameof(Parent))),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        var provider = new Db2QueryProvider<Parent, RowHandle>(parentsFile, model, TableResolver);

        var entityType = model.GetEntityType(typeof(Parent));
        var schema = SchemaResolver(nameof(Parent));
        var table = new Db2Table<Parent, RowHandle>(nameof(Parent), schema, entityType, provider, parentsFile);
        return (table, provider);
    }

    private static Db2TableSchema SchemaResolver(string tableName)
        => tableName switch
        {
            nameof(Parent) => new Db2TableSchema(
                tableName: nameof(Parent),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Level), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),
            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };

    private sealed class Parent
    {
        public int Id { get; set; }
        public int Level { get; set; }
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
