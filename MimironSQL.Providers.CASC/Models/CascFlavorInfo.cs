namespace MimironSQL.Providers;

public static class CascFlavorInfo
{
    public static string ReadProduct(string flavorInfoPath)
    {
        ArgumentNullException.ThrowIfNull(flavorInfoPath);
        if (!File.Exists(flavorInfoPath))
            throw new FileNotFoundException(".flavor.info not found", flavorInfoPath);

        // .flavor.info typically contains a single token like "wow" / "wowt" / etc.
        var text = File.ReadAllText(flavorInfoPath).Trim();
        if (text.Length == 0)
            throw new InvalidDataException(".flavor.info is empty");
        return text;
    }
}
