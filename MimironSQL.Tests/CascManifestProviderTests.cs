using System.Text;

using Microsoft.Extensions.Options;

using MimironSQL.Providers;

using NSubstitute;

using Shouldly;

namespace MimironSQL.Tests;

public sealed class CascManifestProviderTests
{
    [Fact]
    public async Task LocalFirstManifestProvider_resolves_only_entries_with_db2FileDataID()
    {
        var temp = Directory.CreateTempSubdirectory("MimironSQL_CascManifest");
        try
        {
            var manifestJson = """
            [
              {
                "tableName": "AccountStoreCategory",
                "tableHash": "1B5BAF01",
                "db2FileDataID": 6220124
              },
              {
                "tableName": "Achievement",
                "tableHash": "D2EE2CA7",
                "dbcFileDataID": 841607,
                "db2FileDataID": 1260179
              },
              {
                "tableName": "Achievement_Criteria",
                "tableHash": "E3614CCD"
              }
            ]
            """;

            var manifestPath = Path.Combine(temp.FullName, "manifest.json");
            await File.WriteAllTextAsync(manifestPath, manifestJson, Encoding.UTF8);

            var fallback = Substitute.For<IManifestProvider>();
            var options = Options.Create(new WowDb2ManifestOptions { CacheDirectory = temp.FullName, AssetName = "manifest.json" });
            var provider = new LocalFirstManifestProvider(fallback, options);

            (await provider.TryResolveDb2FileDataIdAsync("AccountStoreCategory")).ShouldBe(6220124);
            (await provider.TryResolveDb2FileDataIdAsync("Achievement")).ShouldBe(1260179);
            (await provider.TryResolveDb2FileDataIdAsync("Achievement_Criteria")).ShouldBeNull();
        }
        finally
        {
            temp.Delete(recursive: true);
        }
    }
}
