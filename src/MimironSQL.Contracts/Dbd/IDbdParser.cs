namespace MimironSQL.Dbd;

public interface IDbdParser
{
    IDbdFile Parse(Stream stream);

    IDbdFile Parse(string path);
}
