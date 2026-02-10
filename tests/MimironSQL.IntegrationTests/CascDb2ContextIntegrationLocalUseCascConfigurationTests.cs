using System.Text.Json;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using MimironSQL.Dbd;
using MimironSQL.EntityFrameworkCore;
using MimironSQL.IntegrationTests.Helpers;
using MimironSQL.Providers;

using NSubstitute;

using Shouldly;

namespace MimironSQL.IntegrationTests;

public sealed class CascDb2ContextIntegrationLocalUseCascConfigurationTests
{
    [LocalCascFact]
    public void Can_query_db2context_using_casc_db2_provider_with_delegate_and_di_factories()
    {
        LocalEnvLocal.TryGetWowInstallRoot(out var wowInstallRoot).ShouldBeTrue();
        Directory.Exists(wowInstallRoot).ShouldBeTrue();

        var testDataDir = TestDataPaths.GetTestDataDirectory();
        Directory.Exists(testDataDir).ShouldBeTrue();

        var manifestPath = System.IO.Path.Combine(testDataDir, "manifest.json");
        File.Exists(manifestPath).ShouldBeTrue();

        var optionsBuilder = new DbContextOptionsBuilder<WoWDb2Context>();
        optionsBuilder.UseMimironDb2(o => o.UseCasc(casc =>
        {
            casc.WowInstallRoot = wowInstallRoot;

            casc.ManifestProvider = new JsonFileManifestProvider(manifestPath);

            casc.DbdProviderFactory = sp =>
                new FileSystemDbdProvider(
                    new FileSystemDbdProviderOptions(testDataDir),
                    sp.GetRequiredService<IDbdParser>());
        }));

        using var context = new WoWDb2Context(optionsBuilder.Options);

        var results = context.Map
            .Include(x => x.MapChallengeModes)
            .Where(x => x.MapChallengeModes.Count > 0)
            .Take(10)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.Any(x => x.Id > 0).ShouldBeTrue();
        results.Any(x => !string.IsNullOrWhiteSpace(x.Directory)).ShouldBeTrue();
    }

    [LocalCascFact]
    public void Can_query_db2context_using_casc_db2_provider_with_configuration_binding()
    {
        LocalEnvLocal.TryGetWowInstallRoot(out var wowInstallRoot).ShouldBeTrue();
        Directory.Exists(wowInstallRoot).ShouldBeTrue();

        var testDataDir = TestDataPaths.GetTestDataDirectory();
        Directory.Exists(testDataDir).ShouldBeTrue();

        var manifestPath = System.IO.Path.Combine(testDataDir, "manifest.json");
        File.Exists(manifestPath).ShouldBeTrue();

        var cascSection = Substitute.For<IConfigurationSection>();
        cascSection["WowInstallRoot"].Returns(wowInstallRoot);
        cascSection["DbdDefinitionsDirectory"].Returns(testDataDir);
        cascSection["ManifestDirectory"].Returns(testDataDir);
        cascSection["ManifestAssetName"].Returns("manifest.json");

        var configuration = Substitute.For<IConfiguration>();
        configuration.GetSection("Casc").Returns(cascSection);

        var optionsBuilder = new DbContextOptionsBuilder<WoWDb2Context>();
        optionsBuilder.UseMimironDb2(o => o.UseCasc(configuration));

        using var context = new WoWDb2Context(optionsBuilder.Options);

        var results = context.Map
            .Where(x => x.Id > 0)
            .Take(10)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
    }

    private sealed class JsonFileManifestProvider : IManifestProvider
    {
        private readonly string _manifestPath;

        private readonly SemaphoreSlim _lock = new(1, 1);
        private Dictionary<string, int>? _fileDataIdByTable;

        public JsonFileManifestProvider(string manifestPath)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(manifestPath);
            _manifestPath = manifestPath;
        }

        public Task EnsureManifestExistsAsync(CancellationToken cancellationToken = default)
        {
            if (!File.Exists(_manifestPath))
                throw new FileNotFoundException("Manifest not found", _manifestPath);

            return Task.CompletedTask;
        }

        public async Task<int?> TryResolveDb2FileDataIdAsync(string db2NameOrPath, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(db2NameOrPath))
                return null;

            var map = await GetMapAsync(cancellationToken).ConfigureAwait(false);
            var key = NormalizeToTableName(db2NameOrPath);

            return map.TryGetValue(key, out var fileDataId) ? fileDataId : null;
        }

        private async Task<Dictionary<string, int>> GetMapAsync(CancellationToken cancellationToken)
        {
            if (_fileDataIdByTable is not null)
                return _fileDataIdByTable;

            await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_fileDataIdByTable is not null)
                    return _fileDataIdByTable;

                await using var stream = File.OpenRead(_manifestPath);
                using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);

                var result = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

                if (doc.RootElement.ValueKind != JsonValueKind.Array)
                    throw new InvalidOperationException("Manifest JSON root must be an array.");

                foreach (var element in doc.RootElement.EnumerateArray())
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (!TryGetString(element, "tableName", out var tableName))
                        continue;

                    if (!TryGetInt(element, "db2FileDataID", out var fdid)
                        && !TryGetInt(element, "db2FileDataId", out fdid))
                    {
                        continue;
                    }

                    result[tableName] = fdid;
                }

                _fileDataIdByTable = result;
                return result;
            }
            finally
            {
                _lock.Release();
            }
        }

        private static bool TryGetString(JsonElement element, string propertyName, out string value)
        {
            value = string.Empty;

            if (element.ValueKind != JsonValueKind.Object)
                return false;

            if (!element.TryGetProperty(propertyName, out var prop))
                return false;

            if (prop.ValueKind != JsonValueKind.String)
                return false;

            value = prop.GetString() ?? string.Empty;
            return value.Length > 0;
        }

        private static bool TryGetInt(JsonElement element, string propertyName, out int value)
        {
            value = 0;

            if (element.ValueKind != JsonValueKind.Object)
                return false;

            if (!element.TryGetProperty(propertyName, out var prop))
                return false;

            return prop.ValueKind switch
            {
                JsonValueKind.Number => prop.TryGetInt32(out value),
                JsonValueKind.String => int.TryParse(prop.GetString(), out value),
                _ => false,
            };
        }

        private static string NormalizeToTableName(string db2NameOrPath)
        {
            var value = db2NameOrPath.Trim();

            value = value.Replace('/', '\\');
            if (value.Contains('\\', StringComparison.Ordinal))
            {
                var lastSlash = value.LastIndexOf('\\');
                value = value[(lastSlash + 1)..];
            }

            if (value.EndsWith(".db2", StringComparison.OrdinalIgnoreCase))
                value = value[..^4];

            return value;
        }
    }
}
