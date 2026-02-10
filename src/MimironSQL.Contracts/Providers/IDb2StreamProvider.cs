namespace MimironSQL.Providers;

/// <summary>
/// Opens a DB2 data stream for a named table.
/// </summary>
public interface IDb2StreamProvider
{
    /// <summary>
    /// Opens a readable stream for a DB2 table.
    /// </summary>
    /// <param name="tableName">The DB2 table name (for example, <c>Map</c>).</param>
    /// <returns>A readable stream positioned at the start of the DB2 file content.</returns>
    Stream OpenDb2Stream(string tableName);
}
