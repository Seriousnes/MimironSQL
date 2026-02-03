namespace MimironSQL.Formats;

public readonly struct Db2FileLayout(uint layoutHash, int physicalFieldsCount)
{
    public uint LayoutHash { get; } = layoutHash;
    public int PhysicalFieldsCount { get; } = physicalFieldsCount;
}
