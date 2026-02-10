namespace MimironSQL.Dbd;

/// <inheritdoc />
public sealed class DbdParser : IDbdParser
{
    /// <inheritdoc />
    public IDbdFile Parse(Stream stream)
    {
        if (stream is null)
            throw new ArgumentNullException(nameof(stream));
        return DbdFile.Parse(stream);
    }

    /// <inheritdoc />
    public IDbdFile Parse(string path)
    {
        if (path is null)
            throw new ArgumentNullException(nameof(path));

        using var stream = File.OpenRead(path);
        return Parse(stream);
    }
}
