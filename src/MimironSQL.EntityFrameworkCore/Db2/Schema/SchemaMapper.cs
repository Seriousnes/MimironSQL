using MimironSQL.Db2;
using MimironSQL.Providers;

using System.Globalization;

namespace MimironSQL.EntityFrameworkCore.Db2.Schema;

internal sealed class SchemaMapper(IDbdProvider dbdProvider, string wowVersionRaw)
{
    private readonly WowVersion _wowVersion = WowVersion.Parse(wowVersionRaw);
    private readonly string _wowVersionRaw = wowVersionRaw;

    public Db2TableSchema GetSchema(string tableName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var dbd = dbdProvider.Open(tableName);
        var (build, allowedLayoutHashes, isGlobalBuild) = SelectBuildBlock(tableName, dbd);

        var expectedPhysicalCount = build.GetPhysicalColumnCount();

        var fields = new List<Db2FieldSchema>(build.Entries.Count);
        var physicalIndex = 0;
        foreach (var entry in build.Entries)
        {
            if (entry.IsNonInline)
            {
                var virtualIndex = entry.IsId
                    ? Db2VirtualFieldIndex.Id
                    : entry.IsRelation
                        ? Db2VirtualFieldIndex.ParentRelation
                        : Db2VirtualFieldIndex.UnsupportedNonInline;

                fields.Add(new Db2FieldSchema(
                    entry.Name,
                    entry.ValueType,
                    ColumnStartIndex: virtualIndex,
                    ElementCount: 0,
                    IsVerified: entry.IsVerified,
                    IsVirtual: true,
                    IsId: entry.IsId,
                    IsRelation: entry.IsRelation,
                    ReferencedTableName: entry.ReferencedTableName));
                continue;
            }

            fields.Add(new Db2FieldSchema(
                entry.Name,
                entry.ValueType,
                ColumnStartIndex: physicalIndex,
                ElementCount: entry.ElementCount,
                IsVerified: entry.IsVerified,
                IsVirtual: false,
                IsId: entry.IsId,
                IsRelation: entry.IsRelation,
                ReferencedTableName: entry.ReferencedTableName));

            physicalIndex++;
        }

        if (physicalIndex != expectedPhysicalCount)
        {
            throw new InvalidDataException(
                $"Resolved schema physical column count {physicalIndex} does not match DBD physical column count {expectedPhysicalCount} for {tableName}.dbd.");
        }

        _ = isGlobalBuild;
        return new Db2TableSchema(tableName, physicalIndex, fields, allowedLayoutHashes);
    }

    private (MimironSQL.Dbd.IDbdBuildBlock Build, IReadOnlyList<uint>? AllowedLayoutHashes, bool IsGlobalBuild) SelectBuildBlock(
        string tableName,
        MimironSQL.Dbd.IDbdFile dbd)
    {
        MimironSQL.Dbd.IDbdBuildBlock? bestBuild = null;
        IReadOnlyList<uint>? bestAllowedHashes = null;
        bool bestIsGlobal = false;
        WowVersion? bestCandidate = null;

        foreach (var build in dbd.GlobalBuilds)
        {
            if (!TryGetBestEligibleBuildVersion(build.BuildLine, _wowVersion, out var candidate))
                continue;

            if (bestCandidate is null || candidate.CompareTo(bestCandidate.Value) > 0)
            {
                bestBuild = build;
                bestAllowedHashes = null;
                bestIsGlobal = true;
                bestCandidate = candidate;
                continue;
            }

            if (candidate.CompareTo(bestCandidate.Value) == 0 && bestBuild is not null && !ReferenceEquals(bestBuild, build))
                throw BuildSelectionAmbiguous(tableName, bestBuild.BuildLine, build.BuildLine, candidate);
        }

        foreach (var layout in dbd.Layouts)
        {
            foreach (var build in layout.Builds)
            {
                if (!TryGetBestEligibleBuildVersion(build.BuildLine, _wowVersion, out var candidate))
                    continue;

                if (bestCandidate is null || candidate.CompareTo(bestCandidate.Value) > 0)
                {
                    bestBuild = build;
                    bestAllowedHashes = layout.Hashes;
                    bestIsGlobal = false;
                    bestCandidate = candidate;
                    continue;
                }

                if (candidate.CompareTo(bestCandidate.Value) == 0 && bestBuild is not null && !ReferenceEquals(bestBuild, build))
                    throw BuildSelectionAmbiguous(tableName, bestBuild.BuildLine, build.BuildLine, candidate);
            }
        }

        if (bestBuild is null || bestCandidate is null)
        {
            throw new InvalidDataException(
                $"No compatible BUILD blocks were found for {tableName}.dbd with WOW_VERSION={_wowVersionRaw}." +
                " The DBD definitions may be out of date for the configured WOW_VERSION.");
        }

        return (bestBuild, bestAllowedHashes, bestIsGlobal);
    }

    private InvalidDataException BuildSelectionAmbiguous(string tableName, string leftBuildLine, string rightBuildLine, WowVersion candidate)
    {
        return new InvalidDataException(
            $"Ambiguous BUILD selection for {tableName}.dbd with WOW_VERSION={_wowVersionRaw}. " +
            $"Multiple BUILD blocks match candidate {candidate}: '{leftBuildLine}' and '{rightBuildLine}'.");
    }

    private static bool TryGetBestEligibleBuildVersion(string buildLine, WowVersion requested, out WowVersion best)
    {
        best = default;

        if (string.IsNullOrWhiteSpace(buildLine))
            return false;

        var text = buildLine.Trim();
        if (text.StartsWith("BUILD ", StringComparison.Ordinal))
            text = text.Substring("BUILD ".Length).Trim();

        if (text.Length == 0)
            return false;

        var requestedEffective = requested.GetEffectiveUpperBound();
        WowVersion? currentBest = null;

        foreach (var token in text.Split(',').Select(static t => t.Trim()).Where(static t => t is { Length: > 0 }))
        {
            var dash = token.IndexOf('-');
            if (dash > 0)
            {
                var startText = token.Substring(0, dash).Trim();
                var endText = token.Substring(dash + 1).Trim();

                if (!WowVersion.TryParse(startText, out var start))
                    continue;
                if (!WowVersion.TryParse(endText, out var end))
                    continue;

                if (requestedEffective.CompareTo(start.GetEffectiveUpperBound()) < 0)
                    continue;

                var candidate = requestedEffective.CompareTo(end.GetEffectiveUpperBound()) >= 0
                    ? end.GetEffectiveUpperBound()
                    : requestedEffective;

                if (currentBest is null || candidate.CompareTo(currentBest.Value) > 0)
                    currentBest = candidate;

                continue;
            }

            if (!WowVersion.TryParse(token, out var v))
                continue;

            var candidateV = v.GetEffectiveUpperBound();
            if (candidateV.CompareTo(requestedEffective) > 0)
                continue;

            if (currentBest is null || candidateV.CompareTo(currentBest.Value) > 0)
                currentBest = candidateV;
        }

        if (currentBest is not { } found)
            return false;

        best = found;
        return true;
    }

    private readonly struct WowVersion(int major, int minor, int patch, int build, bool hasBuild) : IComparable<WowVersion>
    {
        public int Major { get; } = major;
        public int Minor { get; } = minor;
        public int Patch { get; } = patch;
        public int Build { get; } = build;
        public bool HasBuild { get; } = hasBuild;

        public static WowVersion Parse(string value)
            => TryParse(value, out var parsed)
                ? parsed
                : throw new InvalidOperationException($"WOW_VERSION value '{value}' is invalid.");

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

        public WowVersion GetEffectiveUpperBound()
            => HasBuild ? this : new WowVersion(Major, Minor, Patch, int.MaxValue, hasBuild: false);

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

        public override string ToString()
            => HasBuild
                ? $"{Major}.{Minor}.{Patch}.{Build}"
                : $"{Major}.{Minor}.{Patch}";
    }
}
