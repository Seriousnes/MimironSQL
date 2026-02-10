namespace MimironSQL.Db2;

/// <summary>
/// Defines reserved virtual field indexes used by the query engine.
/// </summary>
public static class Db2VirtualFieldIndex
{
    /// <summary>
    /// Virtual field index for the row ID.
    /// </summary>
    public const int Id = -1;

    // WDC5 parent relation / section parent lookup.
    /// <summary>
    /// Virtual field index for WDC5 parent relation / section parent lookup.
    /// </summary>
    public const int ParentRelation = -2;

    // Any other non-inline virtual field we don't currently support.
    /// <summary>
    /// Virtual field index for unsupported non-inline virtual fields.
    /// </summary>
    public const int UnsupportedNonInline = -3;
}
