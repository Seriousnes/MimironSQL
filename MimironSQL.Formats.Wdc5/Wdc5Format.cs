using MimironSQL.Db2;
using MimironSQL.Formats;
using MimironSQL.Db2.Wdc5;

namespace MimironSQL.Formats.Wdc5;

public sealed class Wdc5Format : IDb2Format
{
    public static readonly Wdc5Format Instance = new();

    public Db2Format Format => Db2Format.Wdc5;

    public static void Register(Db2FormatRegistry registry)
    {
        ArgumentNullException.ThrowIfNull(registry);
        registry.Register(Instance);
    }

    public IDb2File OpenFile(Stream stream) => new Wdc5File(stream);
}
