namespace MimironSQL.Formats;

public interface IRowHandle
{
    RowHandle Handle { get; }
}

public readonly struct RowHandle(int sectionIndex, int rowIndexInSection, int rowId) : IRowHandle
{
    public int SectionIndex { get; } = sectionIndex;
    public int RowIndexInSection { get; } = rowIndexInSection;
    public int RowId { get; } = rowId;

    public RowHandle Handle => this;
}
