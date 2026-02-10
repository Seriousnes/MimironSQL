using System.Text.Json;

namespace MimironSQL.Providers;

/// <summary>
/// Manifest provider that resolves DB2 table names to FileDataIds from a local manifest JSON file.
/// </summary>
public sealed class FileSystemManifestProvider(CascDb2ProviderOptions options) : IManifestProvider
{
    private readonly CascDb2ProviderOptions _options = options ?? throw new ArgumentNullException(nameof(options));

    private readonly SemaphoreSlim _lock = new(1, 1);
    private Dictionary<string, int>? _db2ByTableName;

    public Task EnsureManifestExistsAsync(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;

        var path = GetManifestPath();
        if (!File.Exists(path))
            throw new FileNotFoundException($"Manifest file not found: '{path}'.", path);

        return Task.CompletedTask;
    }

    public async Task<int?> TryResolveDb2FileDataIdAsync(string db2NameOrPath, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(db2NameOrPath))
            return null;

        var tableName = NormalizeToTableName(db2NameOrPath);
        if (string.IsNullOrWhiteSpace(tableName))
            return null;

        var map = await GetDb2ByTableNameAsync(cancellationToken).ConfigureAwait(false);
        return map.TryGetValue(tableName, out var fdid) ? fdid : null;
    }

    private async Task<IReadOnlyDictionary<string, int>> GetDb2ByTableNameAsync(CancellationToken cancellationToken)
    {
        if (_db2ByTableName is not null)
            return _db2ByTableName;

        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_db2ByTableName is not null)
                return _db2ByTableName;

            await EnsureManifestExistsAsync(cancellationToken).ConfigureAwait(false);

            await using var stream = File.OpenRead(GetManifestPath());
            using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);

            var map = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            if (doc.RootElement.ValueKind == JsonValueKind.Array)
            {
                foreach (var element in doc.RootElement.EnumerateArray())
                {
                    if (element.ValueKind != JsonValueKind.Object)
                        continue;

                    if (!TryReadEntry(element, out var table, out var fdid))
                        continue;

                    map[table] = fdid;
                }
            }
            else if (doc.RootElement.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in doc.RootElement.EnumerateObject())
                {
                    var key = prop.Name.Trim();
                    if (key.Length == 0)
                        continue;

                    var value = prop.Value;
                    if (value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out var fdid))
                    {
                        map[key] = fdid;
                        continue;
                    }

                    if (value.ValueKind == JsonValueKind.Object && TryReadEntry(value, out var table, out fdid))
                        map[table] = fdid;
                }
            }

            _db2ByTableName = map;
            return _db2ByTableName;
        }
        finally
        {
            _lock.Release();
        }
    }

    private string GetManifestPath()
    {
        var directory = !string.IsNullOrWhiteSpace(_options.ManifestCacheDirectory)
            ? _options.ManifestCacheDirectory
            : throw new InvalidOperationException("ManifestCacheDirectory is required when using FileSystemManifestProvider.");

        return Path.Combine(directory, _options.ManifestAssetName);
    }

    private static bool TryReadEntry(JsonElement element, out string tableName, out int fileDataId)
    {
        if (!TryGetStringProperty(element, "tableName", out tableName))
        {
            fileDataId = 0;
            return false;
        }

        if (!TryGetIntProperty(element, "db2FileDataID", out fileDataId)
            && !TryGetIntProperty(element, "db2FileDataId", out fileDataId)
            && !TryGetIntProperty(element, "fileDataId", out fileDataId)
            && !TryGetIntProperty(element, "fdid", out fileDataId))
        {
            return false;
        }

        tableName = tableName.Trim();
        return tableName.Length > 0;
    }

    private static bool TryGetStringProperty(JsonElement element, string name, out string value)
    {
        value = string.Empty;
        if (!element.TryGetProperty(name, out var prop) || prop.ValueKind != JsonValueKind.String)
            return false;

        value = prop.GetString() ?? string.Empty;
        return value.Length > 0;
    }

    private static bool TryGetIntProperty(JsonElement element, string name, out int value)
    {
        value = 0;
        return element.TryGetProperty(name, out var prop)
               && prop.ValueKind == JsonValueKind.Number
               && prop.TryGetInt32(out value);
    }

    private static string NormalizeToTableName(string db2NameOrPath)
    {
        var value = db2NameOrPath.Trim();

        if (value.EndsWith(".db2", StringComparison.OrdinalIgnoreCase))
            value = value[..^4];

        var lastBackslash = value.LastIndexOf('\\');
        var lastSlash = value.LastIndexOf('/');
        var lastSep = Math.Max(lastBackslash, lastSlash);
        if (lastSep >= 0)
            value = value[(lastSep + 1)..];

        return value.Trim();
    }
}
