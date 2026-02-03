namespace MimironSQL.DbContextGenerator;

internal sealed class ManifestMapping(IReadOnlyDictionary<string, int> tableToDb2FileDataId)
{
    public IReadOnlyDictionary<string, int> TableToDb2FileDataId { get; } = tableToDb2FileDataId;

    public static ManifestMapping Empty { get; } = new(new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase));
}
