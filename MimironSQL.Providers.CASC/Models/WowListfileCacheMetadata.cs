namespace MimironSQL.Providers;

public sealed record WowListfileCacheMetadata
{
    public string? Tag { get; init; }

    public string? AssetName { get; init; }

    public string? AssetUrl { get; init; }

    public string? Sha256 { get; init; }

    public DateTimeOffset? DownloadedAtUtc { get; init; }
}
