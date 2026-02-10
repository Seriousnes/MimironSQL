using System.Globalization;

namespace MimironSQL.Providers;

/// <summary>
/// Represents a parsed World of Warcraft version in <c>major.minor.patch.build</c> form.
/// </summary>
/// <param name="Major">The major version.</param>
/// <param name="Minor">The minor version.</param>
/// <param name="Patch">The patch version.</param>
/// <param name="Build">The build number.</param>
internal readonly record struct WowBuildVersion(int Major, int Minor, int Patch, int Build) : IComparable<WowBuildVersion>
{
    /// <summary>
    /// Attempts to parse a version string.
    /// </summary>
    /// <param name="value">The version string.</param>
    /// <param name="version">When this method returns, contains the parsed version.</param>
    /// <returns><see langword="true"/> if parsing succeeded; otherwise <see langword="false"/>.</returns>
    public static bool TryParse(string? value, out WowBuildVersion version)
    {
        version = default;
        if (string.IsNullOrWhiteSpace(value))
            return false;

        // Expected: major.minor.patch.build
        var parts = value.Trim().Split('.', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 4)
            return false;

        if (!int.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out var major))
            return false;
        if (!int.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var minor))
            return false;
        if (!int.TryParse(parts[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out var patch))
            return false;
        if (!int.TryParse(parts[3], NumberStyles.Integer, CultureInfo.InvariantCulture, out var build))
            return false;

        version = new WowBuildVersion(major, minor, patch, build);
        return true;
    }

    /// <summary>
    /// Parses a version string.
    /// </summary>
    /// <param name="value">The version string.</param>
    /// <returns>The parsed version.</returns>
    public static WowBuildVersion Parse(string value)
        => TryParse(value, out var v) ? v : throw new FormatException($"Invalid build version '{value}'. Expected 'major.minor.patch.build'.");

    /// <summary>
    /// Compares this version to another version.
    /// </summary>
    /// <param name="other">The other version.</param>
    /// <returns>
    /// A value less than zero if this instance is less than <paramref name="other"/>,
    /// zero if they are equal, or greater than zero if this instance is greater.
    /// </returns>
    public int CompareTo(WowBuildVersion other)
    {
        var c = Major.CompareTo(other.Major);
        if (c != 0) return c;
        c = Minor.CompareTo(other.Minor);
        if (c != 0) return c;
        c = Patch.CompareTo(other.Patch);
        if (c != 0) return c;
        return Build.CompareTo(other.Build);
    }
}
