namespace MimironSQL.Formats;

public readonly struct RowHandle
{
    public int SectionIndex { get; init; }
    public int RowIndexInSection { get; init; }
    public int RowId { get; init; }
}
