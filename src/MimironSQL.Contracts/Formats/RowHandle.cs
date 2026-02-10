namespace MimironSQL.Formats;

/// <summary>
/// Provides access to a stable <see cref="RowHandle"/> for a row-like object.
/// </summary>
public interface IRowHandle
{
    /// <summary>
    /// Gets the row handle.
    /// </summary>
    RowHandle Handle { get; }
}

/// <summary>
/// Opaque identifier for a row in an opened DB2 file.
/// </summary>
/// <param name="sectionIndex">The zero-based section index.</param>
/// <param name="rowIndexInSection">The zero-based row index within the section.</param>
/// <param name="rowId">The row ID.</param>
public readonly struct RowHandle(int sectionIndex, int rowIndexInSection, int rowId) : IRowHandle
{
    /// <summary>
    /// Gets the zero-based section index.
    /// </summary>
    public int SectionIndex { get; } = sectionIndex;

    /// <summary>
    /// Gets the zero-based row index within the section.
    /// </summary>
    public int RowIndexInSection { get; } = rowIndexInSection;

    /// <summary>
    /// Gets the row ID.
    /// </summary>
    public int RowId { get; } = rowId;

    /// <inheritdoc />
    public RowHandle Handle => this;
}
