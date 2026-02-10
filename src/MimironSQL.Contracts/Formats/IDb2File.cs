using MimironSQL.Db2;

namespace MimironSQL.Formats;

/// <summary>
/// Represents an opened DB2 file.
/// </summary>
public interface IDb2File
{
    /// <summary>
    /// Gets the parsed file header.
    /// </summary>
    IDb2FileHeader Header { get; }

    /// <summary>
    /// Gets the CLR type used to represent rows in this file.
    /// </summary>
    Type RowType { get; }

    /// <summary>
    /// Gets DB2 file flags.
    /// </summary>
    Db2Flags Flags { get; }

    /// <summary>
    /// Gets the number of records in the file.
    /// </summary>
    int RecordsCount { get; }

    /// <summary>
    /// Gets the dense string table bytes, if present.
    /// </summary>
    ReadOnlyMemory<byte> DenseStringTableBytes { get; }

    /// <summary>
    /// Enumerates opaque handles to rows in the file.
    /// </summary>
    /// <returns>A sequence of row handles.</returns>
    IEnumerable<RowHandle> EnumerateRowHandles();

    /// <summary>
    /// Reads a typed field value by field index.
    /// </summary>
    /// <typeparam name="T">The expected field type.</typeparam>
    /// <param name="handle">A row handle obtained from the file.</param>
    /// <param name="fieldIndex">The zero-based field index.</param>
    /// <returns>The field value.</returns>
    T ReadField<T>(RowHandle handle, int fieldIndex);

    /// <summary>
    /// Attempts to resolve a row handle by its ID.
    /// </summary>
    /// <typeparam name="TId">The ID type.</typeparam>
    /// <param name="id">The row ID.</param>
    /// <param name="handle">When successful, receives the row handle.</param>
    /// <returns><see langword="true"/> if the row exists; otherwise <see langword="false"/>.</returns>
    bool TryGetRowHandle<TId>(TId id, out RowHandle handle) where TId : IEquatable<TId>, IComparable<TId>;
}

/// <summary>
/// Represents an opened DB2 file with a strongly typed row representation.
/// </summary>
/// <typeparam name="TRow">The row struct type.</typeparam>
public interface IDb2File<TRow> : IDb2File where TRow : struct
{
    /// <summary>
    /// Enumerates rows in the file.
    /// </summary>
    /// <returns>A sequence of rows.</returns>
    IEnumerable<TRow> EnumerateRows();

    /// <summary>
    /// Attempts to resolve a row by its ID.
    /// </summary>
    /// <typeparam name="TId">The ID type.</typeparam>
    /// <param name="id">The row ID.</param>
    /// <param name="row">When successful, receives the row.</param>
    /// <returns><see langword="true"/> if the row exists; otherwise <see langword="false"/>.</returns>
    bool TryGetRowById<TId>(TId id, out TRow row) where TId : IEquatable<TId>, IComparable<TId>;
}
