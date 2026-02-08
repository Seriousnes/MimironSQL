using System.Buffers;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;

using Microsoft.Extensions.Options;

namespace MimironSQL.Providers;

public sealed class WowDb2ManifestProvider : IWowDb2ManifestProvider, IManifestProvider
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    private readonly HttpClient _httpClient;
    private readonly WowDb2ManifestOptions _options;

    private readonly SemaphoreSlim _db2ByTableNameLock = new(1, 1);
    private Dictionary<string, int>? _db2ByTableName;

    public WowDb2ManifestProvider(HttpClient httpClient, IOptions<WowDb2ManifestOptions> options)
    {
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        
        if (_httpClient.DefaultRequestHeaders.UserAgent.Count == 0)
            _httpClient.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("MimironSQL", "1.0"));

        if (_httpClient.Timeout == Timeout.InfiniteTimeSpan)
            _httpClient.Timeout = TimeSpan.FromSeconds(_options.HttpTimeoutSeconds);
    }

    public async Task EnsureDownloadedAsync(CancellationToken cancellationToken = default)
    {
        var cacheDir = _options.GetCacheDirectoryOrDefault();
        Directory.CreateDirectory(cacheDir);

        var targetPath = Path.Combine(cacheDir, _options.AssetName);
        var metaPath = Path.Combine(cacheDir, _options.AssetName + ".meta.json");

        var release = await GetLatestReleaseAsync(cancellationToken).ConfigureAwait(false);
        var asset = release.Assets.FirstOrDefault(a => string.Equals(a.Name, _options.AssetName, StringComparison.OrdinalIgnoreCase)) ?? throw new InvalidOperationException($"WoWDBDefs latest release did not include asset '{_options.AssetName}'.");
        var expectedSha256 = ParseSha256Digest(asset.Digest) ?? throw new InvalidOperationException($"GitHub release asset '{asset.Name}' did not include a sha256 digest.");
        var existingMeta = await TryReadMetadataAsync(metaPath, cancellationToken).ConfigureAwait(false);
        if (File.Exists(targetPath) && existingMeta?.Sha256 is { Length: > 0 } sha &&
            string.Equals(sha, expectedSha256, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        if (File.Exists(targetPath) && existingMeta is null)
        {
            var fileHash = await ComputeSha256HexAsync(targetPath, cancellationToken).ConfigureAwait(false);
            if (string.Equals(fileHash, expectedSha256, StringComparison.OrdinalIgnoreCase))
            {
                await WriteMetadataAsync(metaPath, new WowDb2ManifestCacheMetadata
                {
                    Tag = release.TagName,
                    AssetName = asset.Name,
                    AssetUrl = asset.BrowserDownloadUrl,
                    Sha256 = expectedSha256,
                    DownloadedAtUtc = DateTimeOffset.UtcNow
                }, cancellationToken).ConfigureAwait(false);

                return;
            }
        }

        var tempPath = targetPath + ".tmp";
        if (File.Exists(tempPath))
            File.Delete(tempPath);

        var downloadedSha = await DownloadToFileAndHashAsync(asset.BrowserDownloadUrl, tempPath, cancellationToken).ConfigureAwait(false);
        if (!string.Equals(downloadedSha, expectedSha256, StringComparison.OrdinalIgnoreCase))
        {
            File.Delete(tempPath);
            throw new InvalidOperationException($"Downloaded manifest sha256 mismatch. Expected {expectedSha256}, got {downloadedSha}.");
        }

        File.Move(tempPath, targetPath, overwrite: true);

        await WriteMetadataAsync(metaPath, new WowDb2ManifestCacheMetadata
        {
            Tag = release.TagName,
            AssetName = asset.Name,
            AssetUrl = asset.BrowserDownloadUrl,
            Sha256 = expectedSha256,
            DownloadedAtUtc = DateTimeOffset.UtcNow
        }, cancellationToken).ConfigureAwait(false);
    }

    public Task EnsureManifestExistsAsync(CancellationToken cancellationToken = default) => EnsureDownloadedAsync(cancellationToken);

    public async Task<Stream> OpenCachedAsync(CancellationToken cancellationToken = default)
    {
        await EnsureDownloadedAsync(cancellationToken).ConfigureAwait(false);

        var cacheDir = _options.GetCacheDirectoryOrDefault();
        var targetPath = Path.Combine(cacheDir, _options.AssetName);
        return File.OpenRead(targetPath);
    }

    public async Task<IReadOnlyDictionary<string, int>> GetDb2FileDataIdByPathAsync(CancellationToken cancellationToken = default)
    {
        var entries = await ReadEntriesAsync(cancellationToken).ConfigureAwait(false);

        var map = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var entry in entries)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (string.IsNullOrWhiteSpace(entry.TableName))
                continue;

            if (entry.Db2FileDataId is not { } db2FileDataId || db2FileDataId <= 0)
                continue;

            var path = CascPath.NormalizeDb2Path($"DBFilesClient\\{entry.TableName}.db2");
            map[path] = db2FileDataId;
        }

        return map;
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

        await _db2ByTableNameLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_db2ByTableName is not null)
                return _db2ByTableName;

            var entries = await ReadEntriesAsync(cancellationToken).ConfigureAwait(false);

            var map = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            foreach (var entry in entries)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (string.IsNullOrWhiteSpace(entry.TableName))
                    continue;

                if (entry.Db2FileDataId is not { } db2FileDataId || db2FileDataId <= 0)
                    continue;

                map[entry.TableName.Trim()] = db2FileDataId;
            }

            _db2ByTableName = map;
            return _db2ByTableName;
        }
        finally
        {
            _db2ByTableNameLock.Release();
        }
    }

    private async Task<List<WoWDbDefsEntryDto>> ReadEntriesAsync(CancellationToken cancellationToken)
    {
        await using var stream = await OpenCachedAsync(cancellationToken).ConfigureAwait(false);
        var entries = await JsonSerializer.DeserializeAsync<List<WoWDbDefsEntryDto>>(stream, JsonOptions, cancellationToken).ConfigureAwait(false);
        return entries ?? [];
    }

    private static string NormalizeToTableName(string db2NameOrPath)
    {
        var value = db2NameOrPath.Trim();

        // Per current contract, callers will provide table names.
        // Accept a minimal subset of path-like inputs for convenience.
        if (value.EndsWith(".db2", StringComparison.OrdinalIgnoreCase))
            value = value[..^4];

        var lastBackslash = value.LastIndexOf('\\');
        var lastSlash = value.LastIndexOf('/');
        var lastSep = Math.Max(lastBackslash, lastSlash);
        if (lastSep >= 0)
            value = value[(lastSep + 1)..];

        return value.Trim();
    }

    private async Task<GitHubReleaseDto> GetLatestReleaseAsync(CancellationToken cancellationToken)
    {
        var url = $"https://api.github.com/repos/{_options.Owner}/{_options.Repository}/releases/latest";

        using var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();

        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        var release = await JsonSerializer.DeserializeAsync<GitHubReleaseDto>(stream, JsonOptions, cancellationToken).ConfigureAwait(false);

        return release ?? throw new InvalidOperationException("GitHub release response was empty.");
    }

    private static string? ParseSha256Digest(string? digest)
    {
        if (string.IsNullOrWhiteSpace(digest))
            return null;

        const string prefix = "sha256:";
        if (!digest.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            return null;

        return digest[prefix.Length..].Trim();
    }

    private static async Task<WowDb2ManifestCacheMetadata?> TryReadMetadataAsync(string path, CancellationToken cancellationToken)
    {
        if (!File.Exists(path))
            return null;

        await using var stream = File.OpenRead(path);
        return await JsonSerializer.DeserializeAsync<WowDb2ManifestCacheMetadata>(stream, JsonOptions, cancellationToken).ConfigureAwait(false);
    }

    private static async Task WriteMetadataAsync(string path, WowDb2ManifestCacheMetadata metadata, CancellationToken cancellationToken)
    {
        await using var stream = File.Open(path, FileMode.Create, FileAccess.Write, FileShare.None);
        await JsonSerializer.SerializeAsync(stream, metadata, JsonOptions, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<string> ComputeSha256HexAsync(string filePath, CancellationToken cancellationToken)
    {
        await using var fs = File.OpenRead(filePath);
        using var sha = SHA256.Create();

        var buffer = ArrayPool<byte>.Shared.Rent(1024 * 128);
        try
        {
            int bytesRead;
            while ((bytesRead = await fs.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken).ConfigureAwait(false)) > 0)
            {
                sha.TransformBlock(buffer, 0, bytesRead, null, 0);
            }

            sha.TransformFinalBlock([], 0, 0);
            return Convert.ToHexString(sha.Hash!).ToLowerInvariant();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private async Task<string> DownloadToFileAndHashAsync(string url, string filePath, CancellationToken cancellationToken)
    {
        using var response = await _httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();

        await using var input = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        await using var output = File.Open(filePath, FileMode.Create, FileAccess.Write, FileShare.None);

        using var sha = SHA256.Create();
        var buffer = ArrayPool<byte>.Shared.Rent(1024 * 128);
        try
        {
            int bytesRead;
            while ((bytesRead = await input.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken).ConfigureAwait(false)) > 0)
            {
                sha.TransformBlock(buffer, 0, bytesRead, null, 0);
                await output.WriteAsync(buffer.AsMemory(0, bytesRead), cancellationToken).ConfigureAwait(false);
            }

            sha.TransformFinalBlock([], 0, 0);
            return Convert.ToHexString(sha.Hash!).ToLowerInvariant();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private sealed record WoWDbDefsEntryDto
    {
        [JsonPropertyName("tableName")]
        public string TableName { get; init; } = string.Empty;

        [JsonPropertyName("tableHash")]
        public string TableHash { get; init; } = string.Empty;

        [JsonPropertyName("dbcFileDataID")]
        public int? DbcFileDataId { get; init; }

        [JsonPropertyName("db2FileDataID")]
        public int? Db2FileDataId { get; init; }
    }

    private sealed record GitHubReleaseDto
    {
        [JsonPropertyName("tag_name")]
        public string TagName { get; init; } = string.Empty;

        [JsonPropertyName("assets")]
        public List<GitHubReleaseAssetDto> Assets { get; init; } = [];
    }

    private sealed record GitHubReleaseAssetDto
    {
        [JsonPropertyName("name")]
        public string Name { get; init; } = string.Empty;

        [JsonPropertyName("browser_download_url")]
        public string BrowserDownloadUrl { get; init; } = string.Empty;

        [JsonPropertyName("digest")]
        public string? Digest { get; init; }
    }
}
