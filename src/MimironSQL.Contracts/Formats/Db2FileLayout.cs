namespace MimironSQL.Formats;

/// <summary>
/// Represents the logical layout identity and physical field count for a DB2 file.
/// </summary>
/// <param name="layoutHash">The resolved layout hash.</param>
/// <param name="physicalFieldsCount">The number of physical fields in the file.</param>
public readonly struct Db2FileLayout(uint layoutHash, int physicalFieldsCount)
{
    /// <summary>
    /// Gets the resolved layout hash.
    /// </summary>
    public uint LayoutHash { get; } = layoutHash;

    /// <summary>
    /// Gets the number of physical fields in the file.
    /// </summary>
    public int PhysicalFieldsCount { get; } = physicalFieldsCount;
}
