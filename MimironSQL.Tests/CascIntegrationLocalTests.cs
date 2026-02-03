using System.Text;
using System.Text.Json;

using Microsoft.Extensions.Options;

using MimironSQL.Providers;

using Shouldly;

namespace MimironSQL.Tests;

public sealed class CascIntegrationLocalTests
{
    [LocalCascFact]
    public async Task CanOpenAnyDb2FromManifestViaCasc()
    {
        LocalEnvLocal.TryGetWowInstallRoot(out var wowInstallRoot).ShouldBeTrue();
        Directory.Exists(wowInstallRoot).ShouldBeTrue();

        var testDataDir = TestDataPaths.GetTestDataDirectory();
        Directory.Exists(testDataDir).ShouldBeTrue();

        var manifestPath = Path.Combine(testDataDir, "manifest.json");
        File.Exists(manifestPath).ShouldBeTrue();

        var candidates = ReadCandidates(manifestPath);
        candidates.Count.ShouldBeGreaterThan(0);

        using var httpClient = new HttpClient();
        var wowDb2ManifestProvider = new WowDb2ManifestProvider(httpClient, Options.Create(new WowDb2ManifestOptions
        {
            CacheDirectory = testDataDir,
            AssetName = "manifest.json",
        }));

        var manifestProvider = new LocalFirstManifestProvider(wowDb2ManifestProvider, Options.Create(new WowDb2ManifestOptions
        {
            CacheDirectory = testDataDir,
            AssetName = "manifest.json",
        }));

        await manifestProvider.EnsureManifestExistsAsync();

        var storage = await CascStorage.OpenInstallRootAsync(wowInstallRoot);
        var provider = new CascDBCProvider(storage, manifestProvider);

        var maxAttempts = Math.Min(25, candidates.Count);
        var attempts = new List<string>(capacity: maxAttempts);

        for (var i = 0; i < maxAttempts; i++)
        {
            var (tableName, fileDataId) = candidates[i];
            attempts.Add($"{tableName} ({fileDataId})");

            try
            {
                await using var stream = provider.OpenDb2Stream(tableName);
                stream.CanRead.ShouldBeTrue();

                var header = new byte[4];
                await stream.ReadExactlyAsync(header);

                Encoding.ASCII.GetString(header).ShouldBe("WDC5");
                return;
            }
            catch
            {
                // Try the next candidate.
            }
        }

        Assert.Fail($"Could not open any DB2 from manifest via CASC. Tried: {string.Join(", ", attempts)}");
    }

    private static List<(string TableName, int FileDataId)> ReadCandidates(string manifestPath)
    {
        using var stream = File.OpenRead(manifestPath);
        using var doc = JsonDocument.Parse(stream);

        var candidates = new List<(string, int)>();

        if (doc.RootElement.ValueKind != JsonValueKind.Array)
            return candidates;

        foreach (var element in doc.RootElement.EnumerateArray())
        {
            if (element.ValueKind != JsonValueKind.Object)
                continue;

            if (!element.TryGetProperty("tableName", out var tableNameProp) || tableNameProp.ValueKind != JsonValueKind.String)
                continue;

            var tableName = (tableNameProp.GetString() ?? string.Empty).Trim();
            if (tableName.Length == 0)
                continue;

            if (!element.TryGetProperty("db2FileDataID", out var fdidProp) || fdidProp.ValueKind != JsonValueKind.Number)
                continue;

            if (!fdidProp.TryGetInt32(out var fileDataId) || fileDataId <= 0)
                continue;

            candidates.Add((tableName, fileDataId));
        }

        return candidates;
    }
}