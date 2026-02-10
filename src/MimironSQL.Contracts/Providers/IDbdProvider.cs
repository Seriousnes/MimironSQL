using MimironSQL.Dbd;

namespace MimironSQL.Providers;

/// <summary>
/// Opens DBD definition files for DB2 tables.
/// </summary>
public interface IDbdProvider
{
    /// <summary>
    /// Opens a parsed DBD definition for the specified table.
    /// </summary>
    /// <param name="tableName">The DB2 table name (for example, <c>Map</c>).</param>
    /// <returns>The parsed DBD definition.</returns>
    IDbdFile Open(string tableName);
}
