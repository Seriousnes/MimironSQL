namespace MimironSQL.Formats;

public interface IRowHandle
{
    RowHandle Handle { get; }
}

public readonly struct RowHandle : IRowHandle
{
    public int SectionIndex { get; }
    public int RowIndexInSection { get; }
    public int RowId { get; }

    public RowHandle(int sectionIndex, int rowIndexInSection, int rowId)
    {
        SectionIndex = sectionIndex;
        RowIndexInSection = rowIndexInSection;
        RowId = rowId;
    }

    public RowHandle Handle => this;
}
