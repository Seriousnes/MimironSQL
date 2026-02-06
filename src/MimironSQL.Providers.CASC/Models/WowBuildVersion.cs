using System.Globalization;

namespace MimironSQL.Providers;

public readonly record struct WowBuildVersion(int Major, int Minor, int Patch, int Build) : IComparable<WowBuildVersion>
{
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

    public static WowBuildVersion Parse(string value)
        => TryParse(value, out var v) ? v : throw new FormatException($"Invalid build version '{value}'. Expected 'major.minor.patch.build'.");

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
