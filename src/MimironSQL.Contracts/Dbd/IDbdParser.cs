namespace MimironSQL.Dbd;

/// <summary>
/// Parses a DBD definition into an <see cref="IDbdFile"/>.
/// </summary>
public interface IDbdParser
{
    /// <summary>
    /// Parses a DBD definition from a stream.
    /// </summary>
    /// <param name="stream">The input stream containing DBD content.</param>
    /// <returns>The parsed DBD file.</returns>
    IDbdFile Parse(Stream stream);

    /// <summary>
    /// Parses a DBD definition from a file path.
    /// </summary>
    /// <param name="path">The path to a DBD definition file.</param>
    /// <returns>The parsed DBD file.</returns>
    IDbdFile Parse(string path);
}
