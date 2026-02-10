using MimironSQL.Db2;

namespace MimironSQL.Formats;

/// <summary>
/// Reads DB2 streams and exposes a format-specific <see cref="IDb2File"/> implementation.
/// </summary>
public interface IDb2Format
{
    /// <summary>
    /// Gets the DB2 format handled by this reader.
    /// </summary>
    Db2Format Format { get; }

    /// <summary>
    /// Opens a DB2 file from a stream.
    /// </summary>
    /// <param name="stream">The input stream containing the DB2 binary data.</param>
    /// <returns>An <see cref="IDb2File"/> representing the DB2 content.</returns>
    IDb2File OpenFile(Stream stream);

    /// <summary>
    /// Gets the logical layout information for a DB2 file.
    /// </summary>
    /// <param name="file">The DB2 file.</param>
    /// <returns>The resolved layout.</returns>
    Db2FileLayout GetLayout(IDb2File file);
}
