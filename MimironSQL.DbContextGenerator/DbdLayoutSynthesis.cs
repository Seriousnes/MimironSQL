using System.Collections.Immutable;
using System.Text;

namespace MimironSQL.DbContextGenerator;

internal static class DbdLayoutSynthesis
{
    internal abstract class BuildConstraint
    {
        public sealed class Exact(string version) : BuildConstraint
        {
            public string Version { get; } = version;
        }

        public sealed class Range(string from, string to) : BuildConstraint
        {
            public string From { get; } = from;
            public string To { get; } = to;
        }
    }

    internal sealed class PhysicalColumn
    {
        public string DbdName { get; }
        public string PropertyName { get; }
        public string DbdType { get; }
        public int? ArrayLength { get; }

        public PhysicalColumn(string dbdName, string propertyName, string dbdType, int? arrayLength)
        {
            DbdName = dbdName;
            PropertyName = propertyName;
            DbdType = dbdType;
            ArrayLength = arrayLength;
        }
    }

    internal sealed class LayoutModel
    {
        public ImmutableArray<BuildConstraint> BuildConstraints { get; }
        public ImmutableArray<PhysicalColumn> PhysicalColumns { get; }

        public LayoutModel(ImmutableArray<BuildConstraint> buildConstraints, ImmutableArray<PhysicalColumn> physicalColumns)
        {
            BuildConstraints = buildConstraints;
            PhysicalColumns = physicalColumns;
        }
    }

    internal sealed class ColumnMaxArrayLengths
    {
        public IReadOnlyDictionary<string, int> MaxByColumnName { get; }

        public ColumnMaxArrayLengths(IReadOnlyDictionary<string, int> maxByColumnName)
        {
            MaxByColumnName = maxByColumnName;
        }
    }

    internal static LayoutModel[] Create(TableSpec table, string[] logicalPropertyNames)
    {
        if (table.Versions.Length == 0)
        {
            var cols = ImmutableArray.CreateBuilder<PhysicalColumn>(table.Columns.Length);
            for (var i = 0; i < table.Columns.Length; i++)
            {
                var c = table.Columns[i];
                cols.Add(new PhysicalColumn(c.Name, logicalPropertyNames[i], c.DbdType, arrayLength: null));
            }

            return
            [
                new LayoutModel(ImmutableArray<BuildConstraint>.Empty, cols.ToImmutable())
            ];
        }

        var logicalNameToProperty = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var logicalNameToType = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < table.Columns.Length; i++)
        {
            logicalNameToProperty[table.Columns[i].Name] = logicalPropertyNames[i];
            logicalNameToType[table.Columns[i].Name] = table.Columns[i].DbdType;
        }

        var signatureToIndex = new Dictionary<string, int>(StringComparer.Ordinal);
        var layouts = new List<(List<BuildConstraint> Builds, ImmutableArray<PhysicalColumn> Columns)>();

        foreach (var version in table.Versions)
        {
            if (version.Columns.Length == 0)
                continue;

            var physical = ImmutableArray.CreateBuilder<PhysicalColumn>(version.Columns.Length);
            var sigBuilder = new StringBuilder();

            foreach (var c in version.Columns)
            {
                var name = c.ColumnName;
                if (!logicalNameToProperty.TryGetValue(name, out var propName))
                    continue;

                var dbdType = logicalNameToType[name];
                physical.Add(new PhysicalColumn(name, propName, dbdType, c.ArrayLength));

                sigBuilder.Append(name);
                sigBuilder.Append(':');
                sigBuilder.Append(c.ArrayLength?.ToString() ?? "-");
                sigBuilder.Append('|');
            }

            var cols = physical.ToImmutable();
            if (cols.Length == 0)
                continue;

            var signature = sigBuilder.ToString();
            if (!signatureToIndex.TryGetValue(signature, out var layoutIndex))
            {
                layoutIndex = layouts.Count;
                signatureToIndex.Add(signature, layoutIndex);
                layouts.Add((new List<BuildConstraint>(), cols));
            }

            var bcList = layouts[layoutIndex].Builds;
            foreach (var b in version.Builds)
            {
                switch (b)
                {
                    case DbdBuildSpec.Exact exact:
                        bcList.Add(new BuildConstraint.Exact(exact.Version));
                        break;
                    case DbdBuildSpec.Range range:
                        bcList.Add(new BuildConstraint.Range(range.From, range.To));
                        break;
                }
            }
        }

        return [.. layouts.Select(static t => new LayoutModel([.. t.Builds], t.Columns))];
    }

    internal static ColumnMaxArrayLengths ComputeMaxArrayLengths(TableSpec table)
    {
        var map = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var version in table.Versions)
        {
            foreach (var col in version.Columns)
            {
                if (col.ArrayLength is not { } len)
                    continue;

                if (!map.TryGetValue(col.ColumnName, out var existing) || len > existing)
                    map[col.ColumnName] = len;
            }
        }

        return new ColumnMaxArrayLengths(map);
    }
}
