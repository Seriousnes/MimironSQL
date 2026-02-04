namespace MimironSQL.Providers;

public sealed record WowDb2ManifestCacheMetadata
{
    public string Tag { get; init; } = string.Empty;

    public string AssetName { get; init; } = string.Empty;

    public string AssetUrl { get; init; } = string.Empty;

    public string Sha256 { get; init; } = string.Empty;

    public DateTimeOffset DownloadedAtUtc { get; init; }
}
