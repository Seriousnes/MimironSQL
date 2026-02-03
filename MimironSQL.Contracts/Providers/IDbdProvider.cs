using MimironSQL.Dbd;

namespace MimironSQL.Providers;

public interface IDbdProvider
{
    IDbdFile Open(string tableName);
}
