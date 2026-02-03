namespace MimironSQL.Formats;

public readonly struct Db2FileLayout
{
	public uint LayoutHash { get; }
	public int PhysicalFieldsCount { get; }

	public Db2FileLayout(uint layoutHash, int physicalFieldsCount)
	{
		LayoutHash = layoutHash;
		PhysicalFieldsCount = physicalFieldsCount;
	}
}
