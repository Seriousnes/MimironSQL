using System.Text.RegularExpressions;

namespace MimironSQL.Providers;

/// <summary>
/// Default implementation of <see cref="IWowBuildIdentityProvider"/>.
/// </summary>
public sealed partial class WowBuildIdentityProvider : IWowBuildIdentityProvider
{
    /// <summary>
    /// Gets build identity information for the installation rooted at <paramref name="installRoot"/>.
    /// </summary>
    /// <param name="installRoot">Root directory of the World of Warcraft installation.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The build identity.</returns>
    public ValueTask<WowBuildIdentity> GetAsync(string installRoot, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(installRoot);

        var layout = CascInstallLayoutDetector.Detect(installRoot);
        var records = CascBuildInfo.Read(layout.BuildInfoPath);
        var record = CascBuildInfo.SelectForProduct(records, layout.Product);

        var buildNumber = TryParseBuildNumber(record.Version);
        var version = record.Version;
        var buildConfigKey = record.BuildConfig;

        var buildKey = buildNumber is { } n
            ? n.ToString()
            : SanitizeForNamespace(version ?? buildConfigKey);

        return new ValueTask<WowBuildIdentity>(new WowBuildIdentity(buildKey, buildNumber, version, buildConfigKey));
    }

    /// <summary>
    /// Attempts to parse a numeric build number from a version string.
    /// </summary>
    /// <param name="version">The version string.</param>
    /// <returns>The build number when found; otherwise <see langword="null"/>.</returns>
    public static int? TryParseBuildNumber(string? version)
    {
        if (string.IsNullOrWhiteSpace(version))
            return null;

        // Example: "11.0.2.58712" -> 58712
        // Some variants include suffixes; find the last digit-run.
        var matches = BuildNumberRegex().Matches(version);
        if (matches.Count == 0)
            return null;

        var last = matches[^1].Value;
        return int.TryParse(last, out var n) ? n : null;
    }

    /// <summary>
    /// Sanitizes an arbitrary string into a token suitable for identifiers.
    /// </summary>
    /// <param name="input">The input string.</param>
    /// <returns>A sanitized token.</returns>
    public static string SanitizeForNamespace(string input)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(input);

        // Replace any non-digit with underscores, collapse multiple underscores.
        var replaced = NonDigitRegex().Replace(input, "_");
        replaced = MultiUnderscoreRegex().Replace(replaced, "_").Trim('_');

        return replaced.Length == 0 ? "unknown" : replaced;
    }

    [GeneratedRegex("\\d+", RegexOptions.CultureInvariant)]
    private static partial Regex BuildNumberRegex();

    [GeneratedRegex("[^0-9]+", RegexOptions.CultureInvariant)]
    private static partial Regex NonDigitRegex();

    [GeneratedRegex("_{2,}", RegexOptions.CultureInvariant)]
    private static partial Regex MultiUnderscoreRegex();
}
