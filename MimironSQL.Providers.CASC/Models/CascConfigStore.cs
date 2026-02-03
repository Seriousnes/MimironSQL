namespace MimironSQL.Providers;

public static class CascConfigStore
{
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
