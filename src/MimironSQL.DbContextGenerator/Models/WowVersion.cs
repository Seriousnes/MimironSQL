using System.Globalization;

namespace MimironSQL.DbContextGenerator.Models;

internal readonly struct WowVersion(int major, int minor, int patch, int build, bool hasBuild) : IComparable<WowVersion>
{
    /// <summary>
    /// Gets the major version component.
    /// </summary>
    public int Major { get; } = major;

    /// <summary>
    /// Gets the minor version component.
    /// </summary>
    public int Minor { get; } = minor;

    /// <summary>
    /// Gets the patch version component.
    /// </summary>
    public int Patch { get; } = patch;

    /// <summary>
    /// Gets the build component.
    /// </summary>
    public int Build { get; } = build;

    /// <summary>
    /// Gets a value indicating whether the build component was explicitly provided.
    /// </summary>
    public bool HasBuild { get; } = hasBuild;

    /// <summary>
    /// Tries to parse a WoW version from the provided text.
    /// </summary>
    /// <param name="value">The text to parse.</param>
    /// <param name="version">The parsed version when the method returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> if parsing succeeded; otherwise <see langword="false"/>.</returns>
    public static bool TryParse(string value, out WowVersion version)
    {
        var rawParts = value.Split(['.'], StringSplitOptions.RemoveEmptyEntries);
        if (rawParts.Length is not (3 or 4))
        {
            version = default;
            return false;
        }

        var majorText = rawParts[0].Trim();
        var minorText = rawParts[1].Trim();
        var patchText = rawParts[2].Trim();

        if (!int.TryParse(majorText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var major) ||
            !int.TryParse(minorText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var minor) ||
            !int.TryParse(patchText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var patch))
        {
            version = default;
            return false;
        }

        if (rawParts.Length == 3)
        {
            version = new WowVersion(major, minor, patch, build: 0, hasBuild: false);
            return true;
        }

        var buildText = rawParts[3].Trim();
        if (!int.TryParse(buildText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var build))
        {
            version = default;
            return false;
        }

        version = new WowVersion(major, minor, patch, build, hasBuild: true);
        return true;
    }

    /// <summary>
    /// Gets an effective upper bound used for range comparisons.
    /// </summary>
    /// <returns>The effective upper bound version.</returns>
    public WowVersion GetEffectiveUpperBound()
        => HasBuild ? this : new WowVersion(Major, Minor, Patch, int.MaxValue, hasBuild: false);

    /// <summary>
    /// Compares this version to another version.
    /// </summary>
    /// <param name="other">The other version.</param>
    /// <returns>
    /// A value less than zero if this instance precedes <paramref name="other"/>, zero if they are equal,
    /// or a value greater than zero if this instance follows <paramref name="other"/>.
    /// </returns>
    public int CompareTo(WowVersion other)
    {
        var major = Major.CompareTo(other.Major);
        if (major != 0) return major;

        var minor = Minor.CompareTo(other.Minor);
        if (minor != 0) return minor;

        var patch = Patch.CompareTo(other.Patch);
        if (patch != 0) return patch;

        return Build.CompareTo(other.Build);
    }
}
