namespace MimironSQL.Providers;

/// <summary>
/// Reads CASC config blobs from the <c>Data\config</c> store.
/// </summary>
internal static class CascConfigStore
{
    /// <summary>
    /// Reads the config blob for the specified key.
    /// </summary>
    /// <param name="dataConfigDirectory">The <c>Data\config</c> directory.</param>
    /// <param name="key">The config blob key.</param>
    /// <returns>The config blob bytes.</returns>
    public static byte[] ReadConfigBytes(string dataConfigDirectory, CascKey key)
    {
        ArgumentNullException.ThrowIfNull(dataConfigDirectory);

        var hex = key.ToString();
        var subDir1 = hex[..2];
        var subDir2 = hex.Substring(2, 2);
        var path = Path.Combine(dataConfigDirectory, subDir1, subDir2, hex);

        if (!File.Exists(path))
            throw new FileNotFoundException("CASC config blob not found", path);

        return File.ReadAllBytes(path);
    }
}
