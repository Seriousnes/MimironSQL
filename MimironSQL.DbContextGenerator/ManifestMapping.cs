namespace CASC.Net.Generators;

internal sealed record ManifestMapping(IReadOnlyDictionary<string, int> TableToDb2FileDataId)
{
    public static ManifestMapping Empty { get; } = new(new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase));
}
