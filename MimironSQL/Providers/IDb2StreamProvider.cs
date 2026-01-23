using System.IO;

namespace MimironSQL.Providers;

public interface IDb2StreamProvider
{
    Stream OpenDb2Stream(string tableName);
}
