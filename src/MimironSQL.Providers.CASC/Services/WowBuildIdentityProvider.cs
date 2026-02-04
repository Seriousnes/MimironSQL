using System.Text.RegularExpressions;

namespace MimironSQL.Providers;

public sealed partial class WowBuildIdentityProvider : IWowBuildIdentityProvider
{
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
