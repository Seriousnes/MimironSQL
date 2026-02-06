namespace MimironSQL.Formats;

public sealed class Db2FormatRegistry
{
    private readonly List<IDb2Format> _formats = [];

    public void Register(IDb2Format format)
    {
        if (format is null)
            throw new ArgumentNullException(nameof(format));
        _formats.Add(format);
    }

    public IReadOnlyList<IDb2Format> Formats => _formats;
}
