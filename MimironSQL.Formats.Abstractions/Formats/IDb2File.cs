using MimironSQL.Db2;

namespace MimironSQL.Formats;

public interface IDb2File
{
    Type RowType { get; }

    Db2Flags Flags { get; }

    int RecordsCount { get; }

    ReadOnlyMemory<byte> DenseStringTableBytes { get; }

    IEnumerable<RowHandle> EnumerateRowHandles();

    T ReadField<T>(RowHandle handle, int fieldIndex);

    bool TryGetRowHandle<TId>(TId id, out RowHandle handle) where TId : IEquatable<TId>, IComparable<TId>;
}

public interface IDb2File<TRow> : IDb2File where TRow : struct
{
    IEnumerable<TRow> EnumerateRows();

    bool TryGetRowById<TId>(TId id, out TRow row) where TId : IEquatable<TId>, IComparable<TId>;
}
