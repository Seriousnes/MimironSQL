namespace MimironSQL.DbContextGenerator;

internal sealed class ColumnSpec
{
	public string DbdType { get; }
	public string Name { get; }
	public int? ArrayLength { get; }

	public ColumnSpec(string dbdType, string name, int? arrayLength)
	{
		DbdType = dbdType;
		Name = name;
		ArrayLength = arrayLength;
	}
}
