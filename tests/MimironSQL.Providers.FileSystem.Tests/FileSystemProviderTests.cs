using System.Text;

using MimironSQL.Dbd;

using Shouldly;

namespace MimironSQL.Providers.FileSystem.Tests;

public sealed class FileSystemProviderTests
{
    [Fact]
    public void FileSystemDb2StreamProvider_OpenDb2Stream_FindsDb2ByTableName()
    {
        var dir = CreateTempDirectory();
        try
        {
            File.WriteAllBytes(Path.Combine(dir, "Spell.db2"), [1, 2, 3]);

            var provider = new FileSystemDb2StreamProvider(new FileSystemDb2StreamProviderOptions(dir));
            using var stream = provider.OpenDb2Stream("spell");
            using var ms = new MemoryStream();
            stream.CopyTo(ms);
            ms.ToArray().ShouldBe([1, 2, 3]);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void FileSystemDb2StreamProvider_OpenDb2Stream_MissingTable_Throws()
    {
        var dir = CreateTempDirectory();
        try
        {
            var provider = new FileSystemDb2StreamProvider(new FileSystemDb2StreamProviderOptions(dir));
            Should.Throw<FileNotFoundException>(() => provider.OpenDb2Stream("DoesNotExist"));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void FileSystemDbdProvider_Open_ParsesDbdFile()
    {
        var dir = CreateTempDirectory();
        try
        {
            File.WriteAllText(Path.Combine(dir, "Spell.dbd"), "COLUMNS\nint ID\n");

            var provider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(dir), new DbdParser());
            var file = provider.Open("Spell");
            file.ColumnsByName.ContainsKey("ID").ShouldBeTrue();
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void FileSystemTactKeyProvider_TryGetKey_ReturnsKeyBytes()
    {
        var dir = CreateTempDirectory();
        try
        {
            var path = Path.Combine(dir, "keys.txt");
            File.WriteAllText(path, "0011223344556677 000102030405060708090A0B0C0D0E0F\n", Encoding.ASCII);

            var provider = new FileSystemTactKeyProvider(new FileSystemTactKeyProviderOptions(path));
            provider.TryGetKey(0x0011223344556677, out var key).ShouldBeTrue();
            key.ToArray().ShouldBe(Convert.FromHexString("000102030405060708090A0B0C0D0E0F"));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    private static string CreateTempDirectory()
    {
        var dir = Path.Combine(Path.GetTempPath(), "MimironSQL-Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }
}
