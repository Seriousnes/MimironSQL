namespace MimironSQL.Providers;

/// <summary>
/// Helpers for reading <c>.flavor.info</c> metadata.
/// </summary>
internal static class CascFlavorInfo
{
    /// <summary>
    /// Reads the CASC product token from a <c>.flavor.info</c> file.
    /// </summary>
    /// <param name="flavorInfoPath">Path to the <c>.flavor.info</c> file.</param>
    /// <returns>The product token.</returns>
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
