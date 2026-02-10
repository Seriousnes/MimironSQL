namespace MimironSQL.Providers;

/// <summary>
/// Identifies a World of Warcraft build and its associated CASC build configuration.
/// </summary>
/// <param name="BuildKey">A sanitized key suitable for identifiers (for example namespace segments).</param>
/// <param name="BuildNumber">The numeric build number, when available.</param>
/// <param name="Version">The version string, when available.</param>
/// <param name="BuildConfigKey">The build config key string.</param>
internal sealed record WowBuildIdentity(
    string BuildKey,
    int? BuildNumber,
    string? Version,
    string BuildConfigKey);
