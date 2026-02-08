using MimironSQL.Db2;
using MimironSQL.Formats.Wdc5.Db2;

namespace MimironSQL.Formats.Wdc5;

public sealed class Wdc5Format : IDb2Format
{
    public Db2Format Format => Db2Format.Wdc5;

    public static void Register(Db2FormatRegistry registry)
    {
        ArgumentNullException.ThrowIfNull(registry);
        registry.Register(new Wdc5Format());
    }

    public IDb2File OpenFile(Stream stream) => new Wdc5File(stream);

    public Db2FileLayout GetLayout(IDb2File file)
    {
        ArgumentNullException.ThrowIfNull(file);
        return new Db2FileLayout(file.Header.LayoutHash, file.Header.FieldsCount);
    }
}
