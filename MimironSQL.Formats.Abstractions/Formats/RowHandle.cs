namespace MimironSQL.Formats;

public interface IRowHandle
{
    RowHandle Handle { get; }
}

public readonly struct RowHandle : IRowHandle
{
    public int SectionIndex { get; init; }
    public int RowIndexInSection { get; init; }
    public int RowId { get; init; }

    public RowHandle Handle => this;
}
