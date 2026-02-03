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

    internal sealed class PhysicalColumn(string dbdName, string propertyName, string dbdType, int? arrayLength)
    {
        public string DbdName { get; } = dbdName;
        public string PropertyName { get; } = propertyName;
        public string DbdType { get; } = dbdType;
        public int? ArrayLength { get; } = arrayLength;
    }

    internal sealed class LayoutModel(ImmutableArray<DbdLayoutSynthesis.BuildConstraint> buildConstraints, ImmutableArray<DbdLayoutSynthesis.PhysicalColumn> physicalColumns)
    {
        public ImmutableArray<BuildConstraint> BuildConstraints { get; } = buildConstraints;
        public ImmutableArray<PhysicalColumn> PhysicalColumns { get; } = physicalColumns;
    }

    internal sealed class ColumnMaxArrayLengths(IReadOnlyDictionary<string, int> maxByColumnName)
    {
        public IReadOnlyDictionary<string, int> MaxByColumnName { get; } = maxByColumnName;
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
                new LayoutModel([], cols.ToImmutable())
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

        foreach (var version in table.Versions.Where(static v => v.Columns.Length != 0))
        {
            var physical = ImmutableArray.CreateBuilder<PhysicalColumn>(version.Columns.Length);
            var sigBuilder = new StringBuilder();

            foreach (var c in version.Columns.Where(c => logicalNameToProperty.ContainsKey(c.ColumnName)))
            {
                var name = c.ColumnName;
                var propName = logicalNameToProperty[name];

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
            foreach (var col in version.Columns.Where(static c => c.ArrayLength is { }))
            {
                var len = col.ArrayLength.GetValueOrDefault();

                if (!map.TryGetValue(col.ColumnName, out var existing) || len > existing)
                    map[col.ColumnName] = len;
            }
        }

        return new ColumnMaxArrayLengths(map);
    }
}
