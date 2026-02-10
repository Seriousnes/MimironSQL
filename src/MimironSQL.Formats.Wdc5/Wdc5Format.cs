using MimironSQL.Db2;
using MimironSQL.Formats.Wdc5.Db2;

namespace MimironSQL.Formats.Wdc5;

/// <summary>
/// DB2 format reader for WDC5 files.
/// </summary>
public sealed class Wdc5Format : IDb2Format
{
    /// <inheritdoc />
    public Db2Format Format => Db2Format.Wdc5;

    /// <inheritdoc />
    public IDb2File OpenFile(Stream stream) => new Wdc5File(stream);

    /// <inheritdoc />
    public Db2FileLayout GetLayout(IDb2File file)
    {
        ArgumentNullException.ThrowIfNull(file);
        return new Db2FileLayout(file.Header.LayoutHash, file.Header.FieldsCount);
    }
}
