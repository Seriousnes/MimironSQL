using MimironSQL.Db2;

namespace MimironSQL.Formats;

public interface IDb2Format
{
    Db2Format Format { get; }

    object OpenFile(Stream stream);

    Db2FileLayout GetLayout(object file);
}
