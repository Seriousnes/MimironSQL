using MimironSQL.Db2;
using MimironSQL.Formats.Wdc5.Db2;
using MimironSQL.Providers;

namespace MimironSQL.Formats.Wdc5;

/// <summary>
/// DB2 format reader for WDC5 files.
/// </summary>
/// <remarks>
/// Creates a new WDC5 format reader.
/// </remarks> 
/// <param name="tactKeyProvider">Optional TACT key provider for decrypting encrypted DB2 files.</param>
public sealed class Wdc5Format(ITactKeyProvider? tactKeyProvider = null) : IDb2Format
{
    private readonly ITactKeyProvider? _tactKeyProvider = tactKeyProvider;

    /// <inheritdoc />
    public Db2Format Format => Db2Format.Wdc5;

    /// <inheritdoc />
    public IDb2File OpenFile(Stream stream)
        => _tactKeyProvider is null
            ? new Wdc5File(stream)
            : new Wdc5File(stream, new Wdc5FileOptions(TactKeyProvider: _tactKeyProvider));

    /// <inheritdoc />
    public Db2FileLayout GetLayout(IDb2File file)
    {
        ArgumentNullException.ThrowIfNull(file);
        return new Db2FileLayout(file.Header.LayoutHash, file.Header.FieldsCount);
    }

    /// <inheritdoc />
    public Db2FileLayout GetLayout(Stream stream) => Wdc5LayoutReader.ReadLayout(stream);
}
