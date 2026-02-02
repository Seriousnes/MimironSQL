using System.Collections.Generic;
using System.Collections.Immutable;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

using MimironSQL.Dbd;
using MimironSQL.Db2;

namespace MimironSQL.DbContextGenerator;

[Generator(LanguageNames.CSharp)]
public sealed class DbContextGenerator : IIncrementalGenerator
{
    private static readonly DiagnosticDescriptor MissingEnvFile = new(
        id: "MSQLDBD001",
        title: "Missing .env file",
        messageFormat: "MimironSQL.DbContextGenerator requires a .env file provided via AdditionalFiles.",
        category: "MimironSQL.DbContextGenerator",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor MissingWowVersion = new(
        id: "MSQLDBD002",
        title: "Missing WOW_VERSION",
        messageFormat: "MimironSQL.DbContextGenerator requires WOW_VERSION=... in .env.",
        category: "MimironSQL.DbContextGenerator",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor InvalidWowVersion = new(
        id: "MSQLDBD003",
        title: "Invalid WOW_VERSION",
        messageFormat: "WOW_VERSION value '{0}' could not be parsed. Expected 'major.minor.patch' or 'major.minor.patch.build'.",
        category: "MimironSQL.DbContextGenerator",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var envProvider = context.AdditionalTextsProvider
            .Where(static f => string.Equals(Path.GetFileName(f.Path), ".env", StringComparison.OrdinalIgnoreCase))
            .Collect()
            .Select(static (files, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (files.Length == 0)
                    return EnvResult.Missing;

                var text = files[0].GetText(cancellationToken);
                if (text is null)
                    return EnvResult.Missing;

                var value = TryReadEnvValue(text, "WOW_VERSION");
                if (value is null)
                    return EnvResult.MissingWowVersion;

                if (!WowVersion.TryParse(value, out var version))
                    return new EnvResult(EnvResultKind.InvalidWowVersion, null, value);

                return new EnvResult(EnvResultKind.Ok, version, value);
            });

        context.RegisterSourceOutput(envProvider, static (spc, env) =>
        {
            if (env.Kind == EnvResultKind.MissingEnv)
                spc.ReportDiagnostic(Diagnostic.Create(MissingEnvFile, Location.None));

            if (env.Kind == EnvResultKind.MissingWowVersion)
                spc.ReportDiagnostic(Diagnostic.Create(MissingWowVersion, Location.None));

            if (env.Kind == EnvResultKind.InvalidWowVersion)
                spc.ReportDiagnostic(Diagnostic.Create(InvalidWowVersion, Location.None, env.RawValue ?? string.Empty));
        });

        var dbdFilesProvider = context.AdditionalTextsProvider
            .Where(static f => f.Path.EndsWith(".dbd", StringComparison.OrdinalIgnoreCase));

        var entitySpecsProvider = dbdFilesProvider
            .Combine(envProvider)
            .Select(static (input, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                var (dbdFileText, env) = input;
                if (env.Kind != EnvResultKind.Ok || env.Version is null)
                    return (EntitySpec?)null;

                var sourceText = dbdFileText.GetText(cancellationToken);
                if (sourceText is null)
                    return null;

                var tableName = Path.GetFileNameWithoutExtension(dbdFileText.Path);
                if (string.IsNullOrWhiteSpace(tableName))
                    return null;

                var dbd = ParseDbd(sourceText);

                if (!TrySelectBuildBlock(dbd, env.Version.Value, out var build))
                    return null;

                var spec = EntitySpec.Create(tableName, dbd, build);
                return spec;
            })
            .Where(static s => s is not null)
            .Select(static (s, _) => s!);

        context.RegisterSourceOutput(entitySpecsProvider.Collect(), static (spc, entities) =>
        {
            if (entities.Length == 0)
                return;

            var byTableName = entities.ToDictionary(e => e.TableName, e => e, StringComparer.Ordinal);

            foreach (var entity in entities.OrderBy(e => e.ClassName, StringComparer.Ordinal))
            {
                var source = RenderEntity(entity, byTableName);
                spc.AddSource($"{entity.ClassName}.g.cs", SourceText.From(source, Encoding.UTF8));
            }

            var contextSource = RenderContext(entities);
            spc.AddSource("WoWDb2Context.g.cs", SourceText.From(contextSource, Encoding.UTF8));
        });
    }

    private static DbdFile ParseDbd(SourceText text)
    {
        var bytes = Encoding.UTF8.GetBytes(text.ToString());
        using var stream = new MemoryStream(bytes, writable: false);
        return DbdFile.Parse(stream);
    }

    private static string? TryReadEnvValue(SourceText envText, string key)
    {
        var prefix = key + "=";
        foreach (var line in envText.Lines)
        {
            var raw = line.ToString().Trim();
            if (raw.StartsWith(prefix, StringComparison.Ordinal))
                return raw[prefix.Length..].Trim();
        }

        return null;
    }

    private static bool TrySelectBuildBlock(DbdFile dbd, WowVersion wowVersion, out DbdBuildBlock build)
    {
        DbdBuildBlock? best = null;
        WowVersion? bestCandidate = null;

        foreach (var b in EnumerateBuildBlocks(dbd))
        {
            if (!TryGetBestEligibleBuildVersion(b.BuildLine, wowVersion, out var candidate))
                continue;

            if (bestCandidate is null || candidate.CompareTo(bestCandidate.Value) > 0)
            {
                best = b;
                bestCandidate = candidate;
            }
        }

        build = best!;
        return best is not null;
    }

    private static IEnumerable<DbdBuildBlock> EnumerateBuildBlocks(DbdFile dbd)
    {
        foreach (var b in dbd.GlobalBuilds)
            yield return b;

        foreach (var layout in dbd.Layouts)
        {
            foreach (var b in layout.Builds)
                yield return b;
        }
    }

    private static bool TryGetBestEligibleBuildVersion(string buildLine, WowVersion requested, out WowVersion best)
    {
        best = default;

        if (string.IsNullOrWhiteSpace(buildLine))
            return false;

        var text = buildLine.Trim();
        if (text.StartsWith("BUILD ", StringComparison.Ordinal))
            text = text["BUILD ".Length..].Trim();

        if (text.Length == 0)
            return false;

        var requestedEffective = requested.GetEffectiveUpperBound();

        WowVersion? currentBest = null;

        foreach (var rawToken in text.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        {
            var token = rawToken.Trim();
            if (token.Length == 0)
                continue;

            var dash = token.IndexOf('-', StringComparison.Ordinal);
            if (dash > 0)
            {
                var startText = token[..dash].Trim();
                var endText = token[(dash + 1)..].Trim();

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

    private static string RenderContext(ImmutableArray<EntitySpec> entities)
    {
        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated />");
        sb.AppendLine();
        sb.AppendLine("using MimironSQL.Db2.Query;");
        sb.AppendLine("using MimironSQL.Providers;");
        sb.AppendLine();
        sb.AppendLine("namespace MimironSQL;");
        sb.AppendLine();
        sb.AppendLine("public partial class WoWDb2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider) : Db2Context(dbdProvider, db2StreamProvider)");
        sb.AppendLine("{");

        foreach (var e in entities.OrderBy(e => e.ClassName, StringComparer.Ordinal))
        {
            sb.AppendLine($"    public Db2Table<{e.ClassName}> {e.ClassName} {{ get; init; }} = null!;");
        }

        sb.AppendLine("}");
        return sb.ToString();
    }

    private static string RenderEntity(EntitySpec entity, IReadOnlyDictionary<string, EntitySpec> byTableName)
    {
        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated />");
        sb.AppendLine();
        sb.AppendLine("using System.Collections.Generic;");
        sb.AppendLine("using System.ComponentModel.DataAnnotations.Schema;");
        sb.AppendLine();
        sb.AppendLine("namespace MimironSQL;");
        sb.AppendLine();

        if (!string.Equals(entity.TableName, entity.ClassName, StringComparison.Ordinal))
            sb.AppendLine($"[Table(\"{EscapeString(entity.TableName)}\")]");

        sb.AppendLine($"public partial class {entity.ClassName} : {entity.BaseType}");
        sb.AppendLine("{");

        foreach (var p in entity.ScalarProperties)
        {
            if (p.ColumnName is not null)
                sb.AppendLine($"    [Column(\"{EscapeString(p.ColumnName)}\")]");

            sb.AppendLine($"    public {p.TypeName} {p.PropertyName} {{ get; set; }}{p.Initializer}");

            sb.AppendLine();
        }

        foreach (var nav in entity.Navigations)
        {
            if (!byTableName.TryGetValue(nav.TargetTableName, out var target))
                continue;

            sb.AppendLine($"    public {target.ClassName}? {nav.PropertyName} {{ get; set; }}");
            sb.AppendLine();
        }

        sb.AppendLine("}");
        return sb.ToString();
    }

    private static string EscapeString(string value)
        => value.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("\"", "\\\"", StringComparison.Ordinal);

    private enum EnvResultKind
    {
        Ok,
        MissingEnv,
        MissingWowVersion,
        InvalidWowVersion,
    }

    private readonly record struct EnvResult(EnvResultKind Kind, WowVersion? Version, string? RawValue)
    {
        public static EnvResult Missing => new(EnvResultKind.MissingEnv, null, null);
        public static EnvResult MissingWowVersion => new(EnvResultKind.MissingWowVersion, null, null);
    }

    private readonly record struct WowVersion(int Major, int Minor, int Patch, int Build, bool HasBuild) : IComparable<WowVersion>
    {
        public static bool TryParse(string value, out WowVersion version)
        {
            var parts = value.Split('.', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length is not (3 or 4))
            {
                version = default;
                return false;
            }

            if (!int.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out var major) ||
                !int.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var minor) ||
                !int.TryParse(parts[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out var patch))
            {
                version = default;
                return false;
            }

            if (parts.Length == 3)
            {
                version = new WowVersion(major, minor, patch, Build: 0, HasBuild: false);
                return true;
            }

            if (!int.TryParse(parts[3], NumberStyles.Integer, CultureInfo.InvariantCulture, out var build))
            {
                version = default;
                return false;
            }

            version = new WowVersion(major, minor, patch, build, HasBuild: true);
            return true;
        }

        public WowVersion GetEffectiveUpperBound()
            => HasBuild ? this : this with { Build = int.MaxValue };

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

    private sealed record EntitySpec(
        string TableName,
        string ClassName,
        string BaseType,
        ImmutableArray<ScalarPropertySpec> ScalarProperties,
        ImmutableArray<NavigationSpec> Navigations)
    {
        public static EntitySpec Create(string tableName, DbdFile dbd, DbdBuildBlock build)
        {
            var className = NameNormalizer.NormalizeTypeName(tableName);

            var idEntry = build.Entries.FirstOrDefault(e => e.IsId);
            var idType = TypeMapping.GetIdClrType(idEntry, dbd.ColumnsByName);
            var baseType = TypeMapping.GetBaseEntityType(idType);

            var scalarProperties = new List<ScalarPropertySpec>();
            var navigations = new List<NavigationSpec>();
            var usedNames = new HashSet<string>(StringComparer.Ordinal);

            foreach (var entry in build.Entries)
            {
                if (entry.IsId)
                    continue;

                var columnName = entry.Name;
                var propertyName = NameNormalizer.NormalizePropertyName(columnName);
                propertyName = NameNormalizer.MakeUnique(propertyName, usedNames);

                var typeName = TypeMapping.GetClrTypeName(entry, dbd.ColumnsByName);
                var initializer = TypeMapping.GetInitializer(typeName, entry);

                string? mappedColumnName = null;
                if (!string.Equals(propertyName, columnName, StringComparison.Ordinal))
                    mappedColumnName = columnName;

                scalarProperties.Add(new ScalarPropertySpec(
                    PropertyName: NameNormalizer.EscapeIdentifier(propertyName),
                    TypeName: typeName,
                    Initializer: initializer,
                    ColumnName: mappedColumnName));

                if (entry is { IsRelation: true, ElementCount: 1 }
                    && entry.ReferencedTableName is { Length: > 0 } targetTable
                    && columnName.EndsWith("ID", StringComparison.Ordinal))
                {
                    var rawNavName = columnName[..^2];
                    var navName = NameNormalizer.NormalizePropertyName(rawNavName);
                    navName = NameNormalizer.MakeUnique(navName, usedNames);

                    navigations.Add(new NavigationSpec(
                        TargetTableName: targetTable,
                        ForeignKeyPropertyName: NameNormalizer.EscapeIdentifier(propertyName),
                        PropertyName: NameNormalizer.EscapeIdentifier(navName)));
                }
            }

            return new EntitySpec(tableName, className, baseType, [.. scalarProperties], [.. navigations]);
        }
    }

    private sealed record ScalarPropertySpec(
        string PropertyName,
        string TypeName,
        string Initializer,
        string? ColumnName);

    private sealed record NavigationSpec(
        string TargetTableName,
        string ForeignKeyPropertyName,
        string PropertyName);

    private static class NameNormalizer
    {
        public static string NormalizeTypeName(string tableName)
        {
            if (tableName.IndexOf('_') < 0)
                return EscapeIdentifier(tableName);

            return EscapeIdentifier(ToPascalCase(tableName));
        }

        public static string NormalizePropertyName(string columnName)
        {
            if (columnName.EndsWith("_lang", StringComparison.Ordinal))
                return ToPascalCase(columnName[..^5]);

            if (columnName.IndexOf('_') >= 0)
                return ToPascalCase(columnName);

            return columnName;
        }

        public static string MakeUnique(string name, HashSet<string> used)
        {
            if (used.Add(name))
                return name;

            for (var i = 2; ; i++)
            {
                var candidate = name + i.ToString(CultureInfo.InvariantCulture);
                if (used.Add(candidate))
                    return candidate;
            }
        }

        public static string EscapeIdentifier(string identifier)
        {
            if (SyntaxFacts.GetKeywordKind(identifier) != SyntaxKind.None)
                return "@" + identifier;

            return identifier;
        }

        private static string ToPascalCase(string value)
        {
            var parts = value.Split('_', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 0)
                return value;

            var sb = new StringBuilder();
            foreach (var part in parts)
            {
                if (part.Length == 0)
                    continue;

                if (part.Equals("ID", StringComparison.Ordinal))
                {
                    sb.Append("ID");
                    continue;
                }

                sb.Append(char.ToUpperInvariant(part[0]));
                if (part.Length > 1)
                    sb.Append(part[1..]);
            }

            return sb.ToString();
        }
    }

    private static class TypeMapping
    {
        public static string GetBaseEntityType(string idClrType)
            => idClrType switch
            {
                "int" => "Db2Entity",
                "long" => "Db2LongEntity",
                "Guid" => "Db2GuidEntity",
                "string" => "Db2StringEntity",
                _ => $"Db2Entity<{idClrType}>",
            };

        public static string GetIdClrType(DbdLayoutEntry idEntry, IReadOnlyDictionary<string, DbdColumn> columnsByName)
        {
            if (idEntry.Name is null)
                return "int";

            if (TryMapInlineInteger(idEntry.InlineTypeToken, out var mapped))
                return mapped;

            if (columnsByName.TryGetValue(idEntry.Name, out var col))
            {
                return col.ValueType switch
                {
                    Db2ValueType.UInt64 => "uint",
                    Db2ValueType.Int64 => "int",
                    Db2ValueType.String or Db2ValueType.LocString => "string",
                    _ => "int",
                };
            }

            return "int";
        }

        public static string GetClrTypeName(DbdLayoutEntry entry, IReadOnlyDictionary<string, DbdColumn> columnsByName)
        {
            var elementType = GetClrElementTypeName(entry, columnsByName);
            return entry.ElementCount > 1 ? $"ICollection<{elementType}>" : elementType;
        }

        private static string GetClrElementTypeName(DbdLayoutEntry entry, IReadOnlyDictionary<string, DbdColumn> columnsByName)
        {
            if (TryMapInlineInteger(entry.InlineTypeToken, out var mapped))
                return mapped;

            return entry.ValueType switch
            {
                Db2ValueType.Single => "float",
                Db2ValueType.String or Db2ValueType.LocString => "string",
                Db2ValueType.UInt64 => "uint",
                Db2ValueType.Int64 => "int",
                _ => "int",
            };
        }

        public static string GetInitializer(string typeName, DbdLayoutEntry entry)
        {
            if (typeName.StartsWith("ICollection<", StringComparison.Ordinal))
                return " = [];";

            if (typeName == "string")
                return " = string.Empty;";

            return string.Empty;
        }

        private static bool TryMapInlineInteger(string? inlineTypeToken, out string clrType)
        {
            clrType = string.Empty;

            if (string.IsNullOrWhiteSpace(inlineTypeToken))
                return false;

            var token = inlineTypeToken.Trim();
            var isUnsigned = token.StartsWith('u');
            var numberText = isUnsigned ? token[1..] : token;

            if (!int.TryParse(numberText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var bits))
                return false;

            clrType = (bits, isUnsigned) switch
            {
                (8, true) => "byte",
                (8, false) => "sbyte",
                (16, true) => "ushort",
                (16, false) => "short",
                (32, true) => "uint",
                (32, false) => "int",
                (64, true) => "ulong",
                (64, false) => "long",
                _ => "int",
            };

            return true;
        }
    }
}
