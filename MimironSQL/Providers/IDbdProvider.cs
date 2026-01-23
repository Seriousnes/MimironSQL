using System.IO;

namespace MimironSQL.Providers;

public interface IDbdProvider
{
    Stream Open(string tableName);
}
