namespace MimironSQL.Providers;

public sealed record WowBuildIdentity(
    string BuildKey,
    int? BuildNumber,
    string? Version,
    string BuildConfigKey);
