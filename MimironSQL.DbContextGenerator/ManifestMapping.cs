namespace MimironSQL.DbContextGenerator;

internal sealed class ManifestMapping
{
    public IReadOnlyDictionary<string, int> TableToDb2FileDataId { get; }

    public ManifestMapping(IReadOnlyDictionary<string, int> tableToDb2FileDataId)
    {
        TableToDb2FileDataId = tableToDb2FileDataId;
    }

    public static ManifestMapping Empty { get; } = new(new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase));
}
