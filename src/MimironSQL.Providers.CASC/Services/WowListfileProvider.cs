using System.Buffers;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text.Json.Serialization;
using System.Text.Json;

using CsvHelper;
using CsvHelper.Configuration;

using Microsoft.Extensions.Options;

namespace MimironSQL.Providers;

public sealed class WowListfileProvider : IWowListfileProvider
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    private readonly HttpClient _httpClient;
    private readonly WowListfileOptions _options;

    public WowListfileProvider(HttpClient httpClient, IOptions<WowListfileOptions> options)
    {
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));

        // GitHub requires a User-Agent.
        if (_httpClient.DefaultRequestHeaders.UserAgent.Count == 0)
            _httpClient.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("CASC.Net", "1.0"));

        if (_httpClient.Timeout == Timeout.InfiniteTimeSpan)
            _httpClient.Timeout = TimeSpan.FromSeconds(_options.HttpTimeoutSeconds);
    }

    public async Task EnsureDownloadedAsync(CancellationToken cancellationToken = default)
    {
        var cacheDir = _options.GetCacheDirectoryOrDefault();
        Directory.CreateDirectory(cacheDir);

        var targetCsvPath = Path.Combine(cacheDir, _options.AssetName);
        var metaPath = Path.Combine(cacheDir, _options.AssetName + ".meta.json");

        var release = await GetLatestReleaseAsync(cancellationToken).ConfigureAwait(false);
        var asset = release.Assets.FirstOrDefault(a => string.Equals(a.Name, _options.AssetName, StringComparison.OrdinalIgnoreCase));
        if (asset is null)
            throw new InvalidOperationException($"wow-listfile latest release did not include asset '{_options.AssetName}'.");

        var expectedSha256 = ParseSha256Digest(asset.Digest);
        if (expectedSha256 is null)
            throw new InvalidOperationException($"GitHub release asset '{asset.Name}' did not include a sha256 digest.");

        var existingMeta = await TryReadMetadataAsync(metaPath, cancellationToken).ConfigureAwait(false);
        if (File.Exists(targetCsvPath) && existingMeta?.Sha256 is { Length: > 0 } sha &&
            string.Equals(sha, expectedSha256, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        if (File.Exists(targetCsvPath) && existingMeta is null)
        {
            var fileHash = await ComputeSha256HexAsync(targetCsvPath, cancellationToken).ConfigureAwait(false);
            if (string.Equals(fileHash, expectedSha256, StringComparison.OrdinalIgnoreCase))
            {
                await WriteMetadataAsync(metaPath, new WowListfileCacheMetadata
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

        var tempPath = targetCsvPath + ".tmp";
        if (File.Exists(tempPath))
            File.Delete(tempPath);

        var downloadedSha = await DownloadToFileAndHashAsync(asset.BrowserDownloadUrl, tempPath, cancellationToken).ConfigureAwait(false);
        if (!string.Equals(downloadedSha, expectedSha256, StringComparison.OrdinalIgnoreCase))
        {
            File.Delete(tempPath);
            throw new InvalidOperationException($"Downloaded listfile sha256 mismatch. Expected {expectedSha256}, got {downloadedSha}.");
        }

        File.Move(tempPath, targetCsvPath, overwrite: true);

        await WriteMetadataAsync(metaPath, new WowListfileCacheMetadata
        {
            Tag = release.TagName,
            AssetName = asset.Name,
            AssetUrl = asset.BrowserDownloadUrl,
            Sha256 = expectedSha256,
            DownloadedAtUtc = DateTimeOffset.UtcNow
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task<Stream> OpenCachedAsync(CancellationToken cancellationToken = default)
    {
        await EnsureDownloadedAsync(cancellationToken).ConfigureAwait(false);

        var cacheDir = _options.GetCacheDirectoryOrDefault();
        var targetCsvPath = Path.Combine(cacheDir, _options.AssetName);

        return File.OpenRead(targetCsvPath);
    }

    public async IAsyncEnumerable<ListfileRecord> ReadRecordsAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var stream = await OpenCachedAsync(cancellationToken).ConfigureAwait(false);
        using var reader = new StreamReader(stream);

        var csvConfig = new CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = false,
            BadDataFound = null,
            MissingFieldFound = null,
            DetectDelimiter = false,
            Delimiter = ",",
            TrimOptions = TrimOptions.Trim,
            IgnoreBlankLines = true
        };

        using var csv = new CsvReader(reader, csvConfig);

        while (await csv.ReadAsync().ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            int fileDataId;
            try
            {
                fileDataId = csv.GetField<int>(0);
            }
            catch
            {
                continue;
            }

            var fileName = csv.GetField(1);
            if (string.IsNullOrWhiteSpace(fileName))
                continue;

            yield return new ListfileRecord { FileDataId = fileDataId, FileName = fileName };
        }
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

    private static async Task<WowListfileCacheMetadata?> TryReadMetadataAsync(string path, CancellationToken cancellationToken)
    {
        if (!File.Exists(path))
            return null;

        await using var stream = File.OpenRead(path);
        return await JsonSerializer.DeserializeAsync<WowListfileCacheMetadata>(stream, JsonOptions, cancellationToken).ConfigureAwait(false);
    }

    private static async Task WriteMetadataAsync(string path, WowListfileCacheMetadata metadata, CancellationToken cancellationToken)
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
