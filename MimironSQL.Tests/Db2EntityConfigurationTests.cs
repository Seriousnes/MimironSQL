using MimironSQL.Db2.Model;
using MimironSQL.Db2.Query;
using MimironSQL.Formats.Wdc5;
using MimironSQL.Providers;

using Shouldly;

namespace MimironSQL.Tests;

public sealed class Db2EntityConfigurationTests
{
    [Fact]
    public void ApplyConfiguration_applies_entity_type_configuration_successfully()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var context = new ConfigurationTestContext(dbdProvider, db2Provider);
        context.EnsureModelCreated();

        var mapEntity = context.Model.GetEntityType(typeof(Fixtures.Map));
        mapEntity.TableName.ShouldBe("Map");
        mapEntity.PrimaryKeyMember.Name.ShouldBe("ID", StringComparer.OrdinalIgnoreCase);

        context.Model.TryGetReferenceNavigation(
            typeof(Fixtures.Map),
            typeof(Fixtures.Map).GetProperty(nameof(Fixtures.Map.ParentMap))!,
            out var nav).ShouldBeTrue();

        nav.SourceKeyFieldSchema.Name.ShouldBe("ParentMapID", StringComparer.OrdinalIgnoreCase);
        nav.TargetKeyFieldSchema.Name.ShouldBe("ID", StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public void Multiple_configurations_via_ApplyConfiguration_work_together()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var context = new AssemblyScanTestContext(dbdProvider, db2Provider);
        context.EnsureModelCreated();

        var mapEntity = context.Model.GetEntityType(typeof(Map));
        mapEntity.TableName.ShouldBe("Map");

        var spellEntity = context.Model.GetEntityType(typeof(Spell));
        spellEntity.TableName.ShouldBe("Spell");
    }

    [Fact]
    public void ApplyConfigurationsFromAssembly_throws_on_multiple_configurations_for_same_entity_type()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var ex = Should.Throw<InvalidOperationException>(() =>
        {
            var context = new DuplicateEntityConfigurationTestContext(dbdProvider, db2Provider);
            context.EnsureModelCreated();
        });

        ex.Message.ShouldContain("Multiple entity type configurations found");
        ex.Message.ShouldContain(typeof(Map).FullName ?? nameof(Map));
    }

    [Fact]
    public void Configuration_precedence_explicit_config_overrides_conventions()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var context = new PrecedenceTestContext(dbdProvider, db2Provider);
        context.EnsureModelCreated();

        var entity = context.Model.GetEntityType(typeof(Map));
        entity.TableName.ShouldBe("Map");
        entity.PrimaryKeyMember.Name.ShouldBe("Id", StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public void Configuration_precedence_later_configuration_overrides_earlier()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var context = new LastWinsTestContext(dbdProvider, db2Provider);
        context.EnsureModelCreated();

        var entity = context.Model.GetEntityType(typeof(Map));
        entity.TableName.ShouldBe("Map");
    }

    [Fact]
    public void Configuration_precedence_OnModelCreating_overrides_ApplyConfiguration()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var context = new OnModelCreatingOverridesTestContext(dbdProvider, db2Provider);
        context.EnsureModelCreated();

        var entity = context.Model.GetEntityType(typeof(Map));
        entity.TableName.ShouldBe("Map");
    }

    [Fact]
    public void Configuration_with_OverridesSchema_prevents_schema_conflicts()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var context = new SchemaOverrideTestContext(dbdProvider, db2Provider);
        context.EnsureModelCreated();

        context.Model.TryGetReferenceNavigation(
            typeof(Fixtures.Map),
            typeof(Fixtures.Map).GetProperty(nameof(Fixtures.Map.ParentMap))!,
            out var nav).ShouldBeTrue();

        nav.OverridesSchema.ShouldBeTrue();
        nav.Kind.ShouldBe(Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne);
    }

    [Fact]
    public void Configuration_conflict_without_OverridesSchema_throws_clear_error()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var ex = Should.Throw<NotSupportedException>(() =>
        {
            var context = new ConflictTestContext(dbdProvider, db2Provider);
            context.EnsureModelCreated();
        });

        ex.Message.ShouldContain("conflicts with schema FK");
        ex.Message.ShouldContain("OverridesSchema()");
    }
}

internal sealed class MapConfiguration : IDb2EntityTypeConfiguration<Fixtures.Map>
{
    public void Configure(Db2EntityTypeBuilder<Fixtures.Map> builder)
        => builder
            .ToTable("Map")
            .HasOne(m => m.ParentMap)
            .WithForeignKey(m => m.ParentMapID);
}

internal sealed class SpellConfiguration : IDb2EntityTypeConfiguration<Spell>
{
    public void Configure(Db2EntityTypeBuilder<Spell> builder)
        => builder.ToTable("Spell");
}

internal sealed class ConfigurationTestContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider, Wdc5Format.Instance)
{
    public Db2Table<Map> Maps { get; init; } = null!;

    public override void OnModelCreating(Db2ModelBuilder modelBuilder)
        => modelBuilder.ApplyConfiguration(new MapConfiguration());
}

internal sealed class AssemblyScanTestContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider, Wdc5Format.Instance)
{
    public Db2Table<Map> Maps { get; init; } = null!;
    public Db2Table<Spell> Spells { get; init; } = null!;

    public override void OnModelCreating(Db2ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfiguration(new MapConfiguration());
        modelBuilder.ApplyConfiguration(new SpellConfiguration());
    }
}

internal sealed class PrecedenceTestContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider, Wdc5Format.Instance)
{
    public Db2Table<Map> Maps { get; init; } = null!;

    public override void OnModelCreating(Db2ModelBuilder modelBuilder)
        => modelBuilder
            .Entity<Map>()
            .ToTable("Map")
            .HasKey(e => e.Id);
}

internal sealed class LastWinsTestContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider, Wdc5Format.Instance)
{
    public Db2Table<Map> Maps { get; init; } = null!;

    public override void OnModelCreating(Db2ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Map>().ToTable("WrongTableName");
        modelBuilder.Entity<Map>().ToTable("Map");
    }
}

internal sealed class OnModelCreatingOverridesTestContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider, Wdc5Format.Instance)
{
    public Db2Table<Map> Maps { get; init; } = null!;

    public override void OnModelCreating(Db2ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfiguration(new WrongMapConfiguration());
        modelBuilder.Entity<Map>().ToTable("Map");
    }
}

internal sealed class WrongMapConfiguration : IDb2EntityTypeConfiguration<Map>
{
    public void Configure(Db2EntityTypeBuilder<Map> builder)
        => builder.ToTable("WrongTableName");
}

internal sealed class SchemaOverrideTestContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider, Wdc5Format.Instance)
{
    public Db2Table<Map> Maps { get; init; } = null!;

    public override void OnModelCreating(Db2ModelBuilder modelBuilder)
        => modelBuilder
            .Entity<Fixtures.Map>()
            .HasOne(m => m.ParentMap)
            .WithSharedPrimaryKey(m => m.ParentMapID, pm => pm.Id)
            .OverridesSchema();
}

internal sealed class ConflictTestContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider, Wdc5Format.Instance)
{
    public Db2Table<Map> Maps { get; init; } = null!;

    public override void OnModelCreating(Db2ModelBuilder modelBuilder)
        => modelBuilder
            .Entity<Fixtures.Map>()
            .HasOne(m => m.ParentMap)
            .WithSharedPrimaryKey(m => m.ParentMapID, pm => pm.Id);
}

internal sealed class DuplicateEntityConfigurationTestContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider, Wdc5Format.Instance)
{
    public Db2Table<Map> Maps { get; init; } = null!;

    public override void OnModelCreating(Db2ModelBuilder modelBuilder)
        => modelBuilder.ApplyConfigurationsFromAssembly(typeof(ErrorTestConfigurations.ConfigurationThatThrows).Assembly);
}

