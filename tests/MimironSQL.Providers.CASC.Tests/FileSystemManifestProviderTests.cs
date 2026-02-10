using Shouldly;

namespace MimironSQL.Providers.CASC.Tests;

public sealed class FileSystemManifestProviderTests
{
    [Fact]
    public async Task EnsureManifestExistsAsync_WhenMissing_Throws()
    {
        var dir = CreateTempDirectory();
        try
        {
            var options = new CascDb2ProviderOptions
            {
                ManifestDirectory = dir,
                ManifestAssetName = "manifest.json",
            };

            var provider = new FileSystemManifestProvider(options);
            await Should.ThrowAsync<FileNotFoundException>(() => provider.EnsureManifestExistsAsync());
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task TryResolveDb2FileDataIdAsync_ResolvesFromArrayManifest()
    {
        var dir = CreateTempDirectory();
        try
        {
            File.WriteAllText(Path.Combine(dir, "manifest.json"), "[\n  {\"tableName\":\"SpellName\",\"db2FileDataId\":123}\n]");

            var options = new CascDb2ProviderOptions
            {
                ManifestDirectory = dir,
                ManifestAssetName = "manifest.json",
            };

            var provider = new FileSystemManifestProvider(options);
            (await provider.TryResolveDb2FileDataIdAsync("SpellName")).ShouldBe(123);
            (await provider.TryResolveDb2FileDataIdAsync("DBFilesClient\\SpellName.db2")).ShouldBe(123);
            (await provider.TryResolveDb2FileDataIdAsync("missing")).ShouldBeNull();
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task TryResolveDb2FileDataIdAsync_ResolvesFromObjectManifest()
    {
        var dir = CreateTempDirectory();
        try
        {
            File.WriteAllText(Path.Combine(dir, "manifest.json"), "{\n  \"SpellName\": 456\n}");

            var options = new CascDb2ProviderOptions
            {
                ManifestDirectory = dir,
                ManifestAssetName = "manifest.json",
            };

            var provider = new FileSystemManifestProvider(options);
            (await provider.TryResolveDb2FileDataIdAsync("spellname")).ShouldBe(456);
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
