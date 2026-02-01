using MimironSQL.Dbd;

namespace MimironSQL.Providers;

public interface IDbdProvider
{
    DbdFile Open(string tableName);
}
