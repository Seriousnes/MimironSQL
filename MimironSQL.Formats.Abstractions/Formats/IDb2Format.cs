using MimironSQL.Db2;

namespace MimironSQL.Formats;

public interface IDb2Format
{
    Db2Format Format { get; }

    IDb2File OpenFile(Stream stream);
}
