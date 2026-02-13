using System.Text.Json;

using MimironSQL.Providers;

namespace MimironSQL.IntegrationTests.Helpers;

sealed class JsonFileManifestProvider : IManifestProvider
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
