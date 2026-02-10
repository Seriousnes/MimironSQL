namespace MimironSQL.Dbd;

public sealed class DbdParser : IDbdParser
{
    public IDbdFile Parse(Stream stream)
    {
        if (stream is null)
            throw new ArgumentNullException(nameof(stream));
        return DbdFile.Parse(stream);
    }

    public IDbdFile Parse(string path)
    {
        if (path is null)
            throw new ArgumentNullException(nameof(path));

        using var stream = File.OpenRead(path);
        return Parse(stream);
    }
}
