using System.Collections.Immutable;
using System.Text;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;

using MimironSQL.Dbd;
using MimironSQL.DbContextGenerator.Utility;
using MimironSQL.DbContextGenerator.Filters;
using MimironSQL.DbContextGenerator.Models;

namespace MimironSQL.DbContextGenerator;

/// <summary>
/// Incremental source generator that emits Entity Framework Core context and entity types from WoWDBDefs <c>.dbd</c> files.
/// </summary>
[Generator(LanguageNames.CSharp)]
public sealed class DbContextGenerator : IIncrementalGenerator
{
    private static readonly DiagnosticDescriptor NoSourcesGenerated = new(
        id: "MSQLDBD004",
        title: "No sources generated",
        messageFormat: "MimironSQL.DbContextGenerator emitted no sources: {0}",
        category: "MimironSQL.DbContextGenerator",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true);

    /// <summary>
    /// Initializes the generator and registers all incremental steps.
    /// </summary>
    /// <param name="context">The generator initialization context.</param>
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var envProvider = context.AdditionalTextsProvider
            .Where(static f =>
            {
                var fileName = Path.GetFileName(f.Path);
                return string.Equals(fileName, ".env", StringComparison.OrdinalIgnoreCase) || string.Equals(fileName, ".env.local", StringComparison.OrdinalIgnoreCase);
            })
            .Collect()
            .Select(static (files, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (files.Length == 0)
                {
                    return EnvResult.Missing;
                }

                AdditionalText? selected = null;
                foreach (var f in files)
                {
                    var fileName = Path.GetFileName(f.Path);
                    if (string.Equals(fileName, ".env.local", StringComparison.OrdinalIgnoreCase))
                    {
                        selected = f;
                        break;
                    }
                }

                selected ??= files[0];

                var text = selected.GetText(cancellationToken);
                if (text is null)
                {
                    return EnvResult.Missing;
                }

                var value = TryReadEnvValue(text, "WOW_VERSION");
                if (value is null)
                {
                    return EnvResult.MissingWowVersion;
                }

                if (!WowVersion.TryParse(value, out var version))
                {
                    return new EnvResult(EnvResultKind.InvalidWowVersion, null, value);
                }

                return new EnvResult(EnvResultKind.Ok, version, value);
            });

        var filterProvider = context.AdditionalTextsProvider
            .Where(static f =>
            {
                var fileName = Path.GetFileName(f.Path);
                return string.Equals(fileName, ".filter", StringComparison.OrdinalIgnoreCase) || string.Equals(fileName, ".filter.local", StringComparison.OrdinalIgnoreCase);
            })
            .Collect()
            .Select(static (files, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (files.Length == 0)
                {
                    return FilterSet.Empty;
                }

                List<string> lines = [];

                foreach (var f in files)
                {
                    var text = f.GetText(cancellationToken);
                    if (text is null)
                    {
                        continue;
                    }

                    foreach (var line in text.Lines.Select(static l => l.ToString()))
                    {
                        lines.Add(line);
                    }
                }

                return FilterSet.Create([.. lines]);
            });

        var dbdFilesProvider = context.AdditionalTextsProvider
            .Where(static f => f.Path.EndsWith(".dbd", StringComparison.OrdinalIgnoreCase));

        var dbdFilesCollectedProvider = dbdFilesProvider.Collect();

        var entitySpecsProvider = dbdFilesProvider
            .Combine(envProvider)
            .Combine(filterProvider)
            .Select(static (input, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                var ((dbdFileText, env), filters) = input;
                if (env.Kind != EnvResultKind.Ok || env.Version is null)
                {
                    return null;
                }

                var sourceText = dbdFileText.GetText(cancellationToken);
                if (sourceText is null)
                {
                    return null;
                }

                var tableName = Path.GetFileNameWithoutExtension(dbdFileText.Path);
                if (string.IsNullOrWhiteSpace(tableName))
                {
                    return null;
                }

                if (!filters.IsAllowed(tableName))
                {
                    return null;
                }

                var dbd = ParseDbd(sourceText);

                if (!TrySelectBuildBlock(dbd, env.Version.Value, out var build))
                {
                    return null;
                }

                var spec = EntitySpec.Create(tableName, dbd, build);
                return spec;
            })
            .Where(static s => s is not null)
            .Select(static (s, _) => s!);

        context.RegisterSourceOutput(entitySpecsProvider.Collect(), static (spc, entities) =>
        {
            if (entities.Length == 0)
            {
                return;
            }

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

        var diagnosticsProvider = envProvider
            .Combine(dbdFilesCollectedProvider)
            .Combine(entitySpecsProvider.Collect())
            .Select(static (input, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                var ((env, dbdFiles), entities) = input;
                return new GenerationStatus(env, dbdFiles.Length, entities.Length);
            });

        context.RegisterSourceOutput(diagnosticsProvider, static (spc, status) =>
        {
            if (status.EntitiesGeneratedCount != 0)
            {
                return;
            }

            var reasons = new List<string>(capacity: 3);

            if (status.Env.Kind == EnvResultKind.MissingEnv)
            {
                reasons.Add("no .env (or .env.local) file was provided via AdditionalFiles");
            }

            if (status.Env.Kind == EnvResultKind.MissingWowVersion)
            {
                reasons.Add("WOW_VERSION is missing from .env");
            }

            if (status.Env.Kind == EnvResultKind.InvalidWowVersion)
            {
                reasons.Add($"WOW_VERSION value '{status.Env.RawValue ?? string.Empty}' is invalid");
            }

            if (status.DbdFilesCount == 0)
            {
                reasons.Add("no .dbd files were provided via AdditionalFiles");
            }

            if (reasons.Count == 0 && status.Env.Kind == EnvResultKind.Ok && status.DbdFilesCount > 0)
            {
                reasons.Add($"no compatible BUILD blocks were found for WOW_VERSION={status.Env.RawValue}");
            }

            if (reasons.Count == 0)
            {
                reasons.Add("inputs were present but no entities could be produced");
            }

            var message = string.Join("; ", reasons);
            spc.ReportDiagnostic(Diagnostic.Create(NoSourcesGenerated, Location.None, message));
        });
    }

    private readonly struct GenerationStatus
    {
        public GenerationStatus(EnvResult env, int dbdFilesCount, int entitiesGeneratedCount)
        {
            Env = env;
            DbdFilesCount = dbdFilesCount;
            EntitiesGeneratedCount = entitiesGeneratedCount;
        }

        public EnvResult Env { get; }

        public int DbdFilesCount { get; }

        public int EntitiesGeneratedCount { get; }
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
            {
                return raw.Substring(prefix.Length).Trim();
            }
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
        {
            yield return b;
        }

        foreach (var layout in dbd.Layouts)
        {
            foreach (var b in layout.Builds)
            {
                yield return b;
            }
        }
    }

    private static bool TryGetBestEligibleBuildVersion(string buildLine, WowVersion requested, out WowVersion best)
    {
        best = default;

        if (string.IsNullOrWhiteSpace(buildLine))
        {
            return false;
        }

        var text = buildLine.Trim();
        if (text.StartsWith("BUILD ", StringComparison.Ordinal))
        {
            text = text.Substring("BUILD ".Length).Trim();
        }

        if (text.Length == 0)
        {
            return false;
        }

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
                {
                    continue;
                }

                if (!WowVersion.TryParse(endText, out var end))
                {
                    continue;
                }

                if (requestedEffective.CompareTo(start.GetEffectiveUpperBound()) < 0)
                {
                    continue;
                }

                var candidate = requestedEffective.CompareTo(end.GetEffectiveUpperBound()) >= 0
                    ? end.GetEffectiveUpperBound()
                    : requestedEffective;

                if (currentBest is null || candidate.CompareTo(currentBest.Value) > 0)
                {
                    currentBest = candidate;
                }

                continue;
            }

            if (!WowVersion.TryParse(token, out var v))
            {
                continue;
            }

            var candidateV = v.GetEffectiveUpperBound();
            if (candidateV.CompareTo(requestedEffective) > 0)
            {
                continue;
            }

            if (currentBest is null || candidateV.CompareTo(currentBest.Value) > 0)
            {
                currentBest = candidateV;
            }
        }

        if (currentBest is not { } found)
        {
            return false;
        }

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

        var usedDbSetNames = new HashSet<string>(StringComparer.Ordinal);

        foreach (var e in entities.OrderBy(e => e.ClassName, StringComparer.Ordinal))
        {
            var dbSetName = e.ClassName.EndsWith("Entity", StringComparison.Ordinal) && e.ClassName.Length > "Entity".Length
                ? e.ClassName.Substring(0, e.ClassName.Length - "Entity".Length)
                : e.ClassName;

            dbSetName = NameNormalizer.MakeUnique(dbSetName, usedDbSetNames);
            sb.AppendLine($"    public DbSet<{e.ClassName}> {dbSetName}");
            sb.AppendLine("    {");
            sb.AppendLine("        get");
            sb.AppendLine("        {");
            sb.AppendLine($"            return field ??= Set<{e.ClassName}>();");
            sb.AppendLine("        }");
            sb.AppendLine("    }");
            sb.AppendLine();
        }

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
        sb.AppendLine("using MimironSQL.EntityFrameworkCore.Model;");
        sb.AppendLine();
        sb.AppendLine("namespace MimironSQL;");
        sb.AppendLine();

        sb.AppendLine($"public partial class {entity.ClassName} : Db2Entity<{entity.IdTypeName}>");
        sb.AppendLine("{");

        var includedNavigations = entity.Navigations
            .Where(n => byTableName.ContainsKey(n.TargetTableName))
            .ToImmutableArray();

        var navsByForeignKey = includedNavigations
            .GroupBy(n => n.ForeignKeyPropertyName, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.ToArray(), StringComparer.Ordinal);

        var fkPropertyTypeByName = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var kvp in navsByForeignKey)
        {
            var fkPropertyName = kvp.Key;
            var navs = kvp.Value;
            var nav = navs[0];
            if (nav is null)
            {
                continue;
            }

            if (!byTableName.TryGetValue(nav.TargetTableName, out var target))
            {
                continue;
            }

            fkPropertyTypeByName[fkPropertyName] = nav.IsForeignKeyArray
                ? $"{target.IdTypeName}[]"
                : target.IdTypeName;
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
            {
                continue;
            }

            foreach (var nav in navs)
            {
                var target = byTableName[nav.TargetTableName];

                if (nav.IsCollection)
                {
                    sb.AppendLine($"    public virtual ICollection<{target.ClassName}> {nav.PropertyName} {{ get; set; }} = [];");
                    sb.AppendLine();
                    continue;
                }

                sb.AppendLine($"    public virtual {target.ClassName}? {nav.PropertyName} {{ get; set; }}");
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

        var includedNavigations = entity.Navigations
            .Where(n => byTableName.ContainsKey(n.TargetTableName))
            .ToImmutableArray();

        if (includedNavigations.Any(static n => n.IsForeignKeyArray))
        {
            sb.AppendLine("using MimironSQL.EntityFrameworkCore.Model;");
        }

        sb.AppendLine();
        sb.AppendLine("namespace MimironSQL;");
        sb.AppendLine();

        var configurationTypeName = entity.ClassName + "Configuration";
        sb.AppendLine($"public partial class {configurationTypeName} : IEntityTypeConfiguration<{entity.ClassName}>");
        sb.AppendLine("{");
        sb.AppendLine($"    public void Configure(EntityTypeBuilder<{entity.ClassName}> builder)");
        sb.AppendLine("    {");
        sb.AppendLine($"        builder.ToTable(\"{entity.TableName.EscapeString()}\");");
        sb.AppendLine("        builder.HasKey(x => x.Id);");
        sb.AppendLine("        builder.Property(x => x.Id).ValueGeneratedNever();");

        if (!string.Equals(entity.IdColumnName, "ID", StringComparison.Ordinal))
        {
            sb.AppendLine($"        builder.Property(x => x.Id).HasColumnName(\"{entity.IdColumnName.EscapeString()}\");");
        }

        foreach (var p in entity.ScalarProperties)
        {
            if (p.ColumnName is null)
            {
                continue;
            }

            sb.AppendLine($"        builder.Property(x => x.{p.PropertyName}).HasColumnName(\"{p.ColumnName.EscapeString()}\");");
        }

        foreach (var nav in includedNavigations)
        {
            _ = byTableName[nav.TargetTableName];

            if (nav.IsForeignKeyArray)
            {
                sb.AppendLine($"        builder.HasMany(x => x.{nav.PropertyName}).WithOne().HasForeignKeyArray(x => x.{nav.ForeignKeyPropertyName});");
                continue;
            }

            if (!nav.IsCollection)
            {
                sb.AppendLine($"        builder.HasOne(x => x.{nav.PropertyName}).WithMany().HasForeignKey(x => x.{nav.ForeignKeyPropertyName});");
            }
        }

        sb.AppendLine("        ConfigureNavigation(builder);");
        sb.AppendLine("    }");
        sb.AppendLine();
        sb.AppendLine($"    partial void ConfigureNavigation(EntityTypeBuilder<{entity.ClassName}> builder);");
        sb.AppendLine("}");

        return sb.ToString();
    }
}
