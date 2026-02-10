namespace MimironSQL.Formats;

/// <summary>
/// Represents the parts of a DB2 file header needed by the query engine.
/// </summary>
public interface IDb2FileHeader
{
    /// <summary>
    /// Gets the resolved layout hash for the file.
    /// </summary>
    uint LayoutHash { get; }

    /// <summary>
    /// Gets the number of logical fields.
    /// </summary>
    int FieldsCount { get; }
}
