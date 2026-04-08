using System.Text;

using MimironSQL.Dbd;

using NSubstitute;

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
    public void FileSystemDbdProvider_Open_DelegatesToParser()
    {
        var dir = CreateTempDirectory();
        try
        {
            File.WriteAllText(Path.Combine(dir, "Spell.dbd"), "COLUMNS\nint ID\n");

            var expectedPath = Path.Combine(dir, "Spell.dbd");

            var expectedFile = Substitute.For<IDbdFile>();
            expectedFile.ColumnsByName.Returns(new Dictionary<string, IDbdColumn>(StringComparer.Ordinal)
            {
                ["ID"] = Substitute.For<IDbdColumn>(),
            });

            var dbdParser = Substitute.For<IDbdParser>();
            dbdParser.Parse(expectedPath).Returns(expectedFile);

            var provider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(dir), dbdParser);
            var file = provider.Open("Spell");

            file.ShouldBeSameAs(expectedFile);
            file.ColumnsByName.ContainsKey("ID").ShouldBeTrue();
            dbdParser.Received(1).Parse(expectedPath);
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

    [Fact]
    public void FileSystemProviderOptions_ConnectionString_ParsesAllKeys()
    {
        var options = new FileSystemProviderOptions(@"Db2DirectoryPath=C:\db2;DbdDefinitionsDirectory=C:\dbd");

        options.Db2DirectoryPath.ShouldBe(@"C:\db2");
        options.DbdDefinitionsDirectory.ShouldBe(@"C:\dbd");
    }

    [Fact]
    public void FileSystemProviderOptions_ConnectionString_SupportsAliases()
    {
        var options = new FileSystemProviderOptions(@"Db2 Directory=C:\db2;Dbd Directory=C:\dbd");

        options.Db2DirectoryPath.ShouldBe(@"C:\db2");
        options.DbdDefinitionsDirectory.ShouldBe(@"C:\dbd");
    }

    [Fact]
    public void FileSystemProviderOptions_ConnectionString_IsCaseInsensitive()
    {
        var options = new FileSystemProviderOptions(@"db2directory=C:\db2;dbddirectory=C:\dbd");

        options.Db2DirectoryPath.ShouldBe(@"C:\db2");
        options.DbdDefinitionsDirectory.ShouldBe(@"C:\dbd");
    }

    [Fact]
    public void FileSystemProviderOptions_ConnectionString_UsesDefaults_ForMissingKeys()
    {
        var options = new FileSystemProviderOptions(@"Db2Directory=C:\db2");

        options.Db2DirectoryPath.ShouldBe(@"C:\db2");
        options.DbdDefinitionsDirectory.ShouldBe(string.Empty);
    }

    private static string CreateTempDirectory()
    {
        var dir = Path.Combine(Path.GetTempPath(), "MimironSQL-Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }
}
