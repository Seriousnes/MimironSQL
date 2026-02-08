using System.Collections.Immutable;
using System.Globalization;
using System.Text;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
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
        messageFormat: "MimironSQL.DbContextGenerator requires a .env file provided via AdditionalFiles",
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
                    return null;

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

                var configurationSource = RenderEntityConfiguration(entity, byTableName);
                spc.AddSource($"{entity.ClassName}Configuration.g.cs", SourceText.From(configurationSource, Encoding.UTF8));
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
        foreach (var raw in envText.Lines.Select(static l => l.ToString().Trim()))
        {
            if (raw.StartsWith(prefix, StringComparison.Ordinal))
                return raw.Substring(prefix.Length).Trim();
        }

        return null;
    }

    private static bool TrySelectBuildBlock(DbdFile dbd, WowVersion wowVersion, out DbdBuildBlock build)
    {
        DbdBuildBlock? best = null;
        WowVersion? bestCandidate = null;

        foreach (var b in EnumerateBuildBlocks(dbd).Where(b => TryGetBestEligibleBuildVersion(b.BuildLine, wowVersion, out _)))
        {
            TryGetBestEligibleBuildVersion(b.BuildLine, wowVersion, out var candidate);

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

    private static string RenderContext(ImmutableArray<EntitySpec> entities)
    {
        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated />");
        sb.AppendLine();
        sb.AppendLine("using Microsoft.EntityFrameworkCore;");
        sb.AppendLine();
        sb.AppendLine("namespace MimironSQL;");
        sb.AppendLine();
        sb.AppendLine("public partial class WoWDb2Context(DbContextOptions<WoWDb2Context> options) : DbContext(options)");
        sb.AppendLine("{");

        foreach (var e in entities.OrderBy(e => e.ClassName, StringComparer.Ordinal))
        {
            sb.AppendLine($"    public DbSet<{e.ClassName}> {e.ClassName} => Set<{e.ClassName}>();");
        }

        sb.AppendLine();
        sb.AppendLine("    protected override void OnModelCreating(ModelBuilder modelBuilder)");
        sb.AppendLine("    {");
        sb.AppendLine("        base.OnModelCreating(modelBuilder);");
        sb.AppendLine();

        sb.AppendLine("        modelBuilder.ApplyConfigurationsFromAssembly(typeof(WoWDb2Context).Assembly);");
        sb.AppendLine();

        sb.AppendLine("        OnModelCreatingPartial(modelBuilder);");
        sb.AppendLine("    }");
        sb.AppendLine();
        sb.AppendLine("    partial void OnModelCreatingPartial(ModelBuilder modelBuilder);");

        sb.AppendLine("}");
        return sb.ToString();
    }

    private static string RenderEntity(EntitySpec entity, IReadOnlyDictionary<string, EntitySpec> byTableName)
    {
        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated />");
        sb.AppendLine("#nullable enable");
        sb.AppendLine();
        sb.AppendLine("using System.Collections.Generic;");
        sb.AppendLine();
        sb.AppendLine("namespace MimironSQL;");
        sb.AppendLine();

        sb.AppendLine($"public partial class {entity.ClassName}");
        sb.AppendLine("{");

        var idInitializer = TypeMapping.GetInitializer(entity.IdTypeName);
        sb.AppendLine($"    public {entity.IdTypeName} Id {{ get; set; }}{idInitializer}");
        sb.AppendLine();

        var navsByForeignKey = entity.Navigations
            .Where(n => byTableName.ContainsKey(n.TargetTableName))
            .GroupBy(n => n.ForeignKeyPropertyName, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.ToArray(), StringComparer.Ordinal);

        var fkPropertyTypeByName = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var kvp in navsByForeignKey)
        {
            var fkPropertyName = kvp.Key;
            var navs = kvp.Value;
            var nav = navs.FirstOrDefault(static n => !n.IsCollection);
            if (nav is null)
                continue;

            if (!byTableName.TryGetValue(nav.TargetTableName, out var target))
                continue;

            fkPropertyTypeByName[fkPropertyName] = target.IdTypeName;
        }

        foreach (var p in entity.ScalarProperties)
        {
            var effectiveTypeName = fkPropertyTypeByName.TryGetValue(p.PropertyName, out var fkType)
                ? fkType
                : p.TypeName;

            var initializer = string.Equals(effectiveTypeName, p.TypeName, StringComparison.Ordinal)
                ? p.Initializer
                : TypeMapping.GetInitializer(effectiveTypeName);

            sb.AppendLine($"    public {effectiveTypeName} {p.PropertyName} {{ get; set; }}{initializer}");

            sb.AppendLine();

            if (!navsByForeignKey.TryGetValue(p.PropertyName, out var navs))
                continue;

            foreach (var nav in navs)
            {
                var target = byTableName[nav.TargetTableName];

                if (nav.IsCollection)
                    continue;

                sb.AppendLine($"    public {target.ClassName}? {nav.PropertyName} {{ get; set; }}" );

                sb.AppendLine();
            }
        }

        sb.AppendLine("}");
        return sb.ToString();
    }

    private static string RenderEntityConfiguration(EntitySpec entity, IReadOnlyDictionary<string, EntitySpec> byTableName)
    {
        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated />");
        sb.AppendLine();
        sb.AppendLine("using Microsoft.EntityFrameworkCore;");
        sb.AppendLine("using Microsoft.EntityFrameworkCore.Metadata.Builders;");
        sb.AppendLine();
        sb.AppendLine("namespace MimironSQL;");
        sb.AppendLine();

        var configurationTypeName = entity.ClassName + "Configuration";
        sb.AppendLine($"public partial class {configurationTypeName} : IEntityTypeConfiguration<{entity.ClassName}>");
        sb.AppendLine("{");
        sb.AppendLine($"    public void Configure(EntityTypeBuilder<{entity.ClassName}> builder)");
        sb.AppendLine("    {");
        sb.AppendLine($"        builder.ToTable(\"{EscapeString(entity.TableName)}\");");
        sb.AppendLine("        builder.HasKey(x => x.Id);");

        foreach (var p in entity.ScalarProperties)
        {
            if (p.ColumnName is null)
                continue;

            sb.AppendLine($"        builder.Property(x => x.{p.PropertyName}).HasColumnName(\"{EscapeString(p.ColumnName)}\");");
        }

        foreach (var nav in entity.Navigations)
        {
            if (nav.IsCollection)
                continue;

            if (!byTableName.TryGetValue(nav.TargetTableName, out var target))
                continue;

            _ = target;
            sb.AppendLine($"        builder.HasOne(x => x.{nav.PropertyName}).WithMany().HasForeignKey(x => x.{nav.ForeignKeyPropertyName});");
        }

        sb.AppendLine("        ConfigureNavigation(builder);");
        sb.AppendLine("    }");
        sb.AppendLine();
        sb.AppendLine($"    partial void ConfigureNavigation(EntityTypeBuilder<{entity.ClassName}> builder);");
        sb.AppendLine("}");

        return sb.ToString();
    }

    private static string EscapeString(string value)
        => value.Replace("\\", "\\\\").Replace("\"", "\\\"");

    private enum EnvResultKind
    {
        Ok,
        MissingEnv,
        MissingWowVersion,
        InvalidWowVersion,
    }

    private readonly struct EnvResult(DbContextGenerator.EnvResultKind kind, DbContextGenerator.WowVersion? version, string? rawValue)
    {
        public EnvResultKind Kind { get; } = kind;
        public WowVersion? Version { get; } = version;
        public string? RawValue { get; } = rawValue;

        public static EnvResult Missing => new(EnvResultKind.MissingEnv, null, null);
        public static EnvResult MissingWowVersion => new(EnvResultKind.MissingWowVersion, null, null);
    }

    private readonly struct WowVersion(int major, int minor, int patch, int build, bool hasBuild) : IComparable<WowVersion>
    {
        public int Major { get; } = major;
        public int Minor { get; } = minor;
        public int Patch { get; } = patch;
        public int Build { get; } = build;
        public bool HasBuild { get; } = hasBuild;

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
    }

    private sealed class EntitySpec(
        string tableName,
        string className,
        string idTypeName,
        ImmutableArray<DbContextGenerator.ScalarPropertySpec> scalarProperties,
        ImmutableArray<DbContextGenerator.NavigationSpec> navigations)
    {
        public string TableName { get; } = tableName;
        public string ClassName { get; } = className;
        public string IdTypeName { get; } = idTypeName;
        public ImmutableArray<ScalarPropertySpec> ScalarProperties { get; } = scalarProperties;
        public ImmutableArray<NavigationSpec> Navigations { get; } = navigations;

        public static EntitySpec Create(string tableName, DbdFile dbd, DbdBuildBlock build)
        {
            var className = NameNormalizer.NormalizeTypeName(tableName);

            var idEntry = build.Entries.FirstOrDefault(e => e.IsId);
            var idType = TypeMapping.GetIdClrType(idEntry, dbd.ColumnsByName);

            var scalarProperties = new List<ScalarPropertySpec>();
            var navigations = new List<NavigationSpec>();
            var usedNames = new HashSet<string>(StringComparer.Ordinal);
            var scalarPropertyNameByColumnName = new Dictionary<string, (string EscapedPropertyName, string UnescapedPropertyName)>(StringComparer.Ordinal);

            foreach (var entry in build.Entries.Where(static e => !e.IsId && e.ElementCount == 1))
            {
                var columnName = entry.Name;
                var propertyName = NameNormalizer.NormalizePropertyName(columnName);
                propertyName = NameNormalizer.MakeUnique(propertyName, usedNames);

                var typeName = TypeMapping.GetClrTypeName(entry);
                var initializer = TypeMapping.GetInitializer(typeName);

                string? mappedColumnName = null;
                if (!string.Equals(propertyName, columnName, StringComparison.Ordinal))
                    mappedColumnName = columnName;

                var escapedPropertyName = NameNormalizer.EscapeIdentifier(propertyName);

                scalarProperties.Add(new ScalarPropertySpec(
                    escapedPropertyName,
                    typeName,
                    initializer,
                    mappedColumnName));

                scalarPropertyNameByColumnName[columnName] = (escapedPropertyName, propertyName);
            }

            foreach (var entry in build.Entries.Where(static e => !e.IsId && e.ElementCount == 1))
            {
                if (entry.ReferencedTableName is not { Length: > 0 } targetTable)
                    continue;

                var columnName = entry.Name;
                if (!scalarPropertyNameByColumnName.TryGetValue(columnName, out var scalarProperty))
                    continue;

                var rawNavName = columnName.EndsWith("ID", StringComparison.Ordinal)
                    ? columnName.Substring(0, columnName.Length - 2)
                    : columnName;

                var navName = NameNormalizer.NormalizePropertyName(rawNavName);
                if (string.Equals(navName, scalarProperty.UnescapedPropertyName, StringComparison.Ordinal))
                {
                    navName += entry.ElementCount > 1 ? "Collection" : "Entity";
                }

                navName = NameNormalizer.MakeUnique(navName, usedNames);

                navigations.Add(new NavigationSpec(
                    targetTableName: targetTable,
                    foreignKeyPropertyName: scalarProperty.EscapedPropertyName,
                    propertyName: NameNormalizer.EscapeIdentifier(navName),
                    isCollection: false));
            }

            return new EntitySpec(tableName, className, idType, [.. scalarProperties], [.. navigations]);
        }
    }

    private sealed class ScalarPropertySpec(string propertyName, string typeName, string initializer, string? columnName)
    {
        public string PropertyName { get; } = propertyName;
        public string TypeName { get; } = typeName;
        public string Initializer { get; } = initializer;
        public string? ColumnName { get; } = columnName;
    }

    private sealed class NavigationSpec(string targetTableName, string foreignKeyPropertyName, string propertyName, bool isCollection)
    {
        public string TargetTableName { get; } = targetTableName;
        public string ForeignKeyPropertyName { get; } = foreignKeyPropertyName;
        public string PropertyName { get; } = propertyName;
        public bool IsCollection { get; } = isCollection;
    }

    private static class NameNormalizer
    {
        public static string NormalizeTypeName(string tableName)
        {
            return tableName.IndexOf('_') switch
            {
                < 0 => EscapeIdentifier(tableName),
                _ => EscapeIdentifier(ToPascalCase(tableName)),
            };
        }

        public static string NormalizePropertyName(string columnName)
        {
            // Don't normalize Field_X_Y_Z style columns
            if (columnName.StartsWith("Field_", StringComparison.InvariantCultureIgnoreCase))
                return columnName;

            if (columnName.EndsWith("_lang", StringComparison.Ordinal))
                return ToPascalCase(columnName.Substring(0, columnName.Length - 5));

            return columnName.IndexOf('_') switch
            {
                >= 0 => ToPascalCase(columnName),
                _ => columnName,
            };
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
            var parts = value.Split(['_'], StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 0)
                return value;

            var sb = new StringBuilder();
            foreach (var part in parts.Where(static p => p is { Length: > 0 }))
            {
                if (part.Equals("ID", StringComparison.Ordinal))
                {
                    sb.Append("ID");
                    continue;
                }

                sb.Append(char.ToUpperInvariant(part[0]));
                if (part.Length > 1)
                    sb.Append(part.Substring(1));
            }

            return sb.ToString();
        }
    }

    private static class TypeMapping
    {
        public static string GetIdClrType(DbdLayoutEntry idEntry, IReadOnlyDictionary<string, DbdColumn> columnsByName)
        {
            if (idEntry.Name is null)
                return "int";

            if (TryMapInlineInteger(idEntry.InlineTypeToken, out var mapped))
                return PromoteUnsignedKeyType(mapped);

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

        private static string PromoteUnsignedKeyType(string typeName)
        {
            return typeName switch
            {
                "byte" => "short",
                "ushort" => "int",
                "uint" => "long",
                _ => typeName,
            };
        }

        public static string GetClrTypeName(DbdLayoutEntry entry)
        {
            var elementType = GetClrElementTypeName(entry);
            return entry.ElementCount > 1 ? $"ICollection<{elementType}>" : elementType;
        }

        private static string GetClrElementTypeName(DbdLayoutEntry entry)
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

        public static string GetInitializer(string typeName)
        {
            if (typeName.StartsWith("ICollection<", StringComparison.Ordinal))
                return " = [];";

            return typeName switch
            {
                "string" => " = string.Empty;",
                _ => string.Empty,
            };
        }

        private static bool TryMapInlineInteger(string? inlineTypeToken, out string clrType)
        {
            clrType = string.Empty;

            if (inlineTypeToken is null)
                return false;

            var token = inlineTypeToken.Trim();
            if (token.Length == 0)
                return false;

            var isUnsigned = token.StartsWith("u", StringComparison.Ordinal);
            var numberText = isUnsigned ? token.Substring(1) : token;

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
