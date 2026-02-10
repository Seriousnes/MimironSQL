namespace MimironSQL.Formats;

/// <summary>
/// Registers and exposes available DB2 format readers.
/// </summary>
public sealed class Db2FormatRegistry
{
    private readonly List<IDb2Format> _formats = [];

    /// <summary>
    /// Registers a DB2 format reader.
    /// </summary>
    /// <param name="format">The format reader to register.</param>
    public void Register(IDb2Format format)
    {
        if (format is null)
            throw new ArgumentNullException(nameof(format));
        _formats.Add(format);
    }

    /// <summary>
    /// Gets the registered DB2 format readers.
    /// </summary>
    public IReadOnlyList<IDb2Format> Formats => _formats;
}
