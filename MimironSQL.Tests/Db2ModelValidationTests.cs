using MimironSQL.Db2.Model;
using MimironSQL.Db2.Query;
using MimironSQL.Providers;
using MimironSQL.Tests.Fixtures;

using Shouldly;

namespace MimironSQL.Tests;

public sealed class Db2ModelValidationTests
{
    [Fact]
    public void Model_build_validates_primary_key_field_exists_in_schema()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var ex = Should.Throw<NotSupportedException>(() =>
            _ = new MissingPkFieldInSchemaTestContext(dbdProvider, db2Provider));

        ex.Message.ShouldContain("Field 'NonExistentKey' not found in schema");
        ex.Message.ShouldContain("primary key member");
        ex.Message.ShouldContain("EntityWithNonExistentPkField");
    }

    [Fact]
    public void Model_build_validates_foreign_key_field_exists_in_schema()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var ex = Should.Throw<NotSupportedException>(() =>
            _ = new MissingFkFieldInSchemaTestContext(dbdProvider, db2Provider));

        ex.Message.ShouldContain("Field 'NonExistentForeignKey' not found in schema");
        ex.Message.ShouldContain("source key member");
        ex.Message.ShouldContain("EntityWithNonExistentFkField");
    }

    [Fact]
    public void Model_build_validates_target_primary_key_field_exists_in_schema()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var ex = Should.Throw<NotSupportedException>(() =>
            _ = new MissingTargetPkFieldInSchemaTestContext(dbdProvider, db2Provider));

        ex.Message.ShouldContain("Field 'FakeTargetKey' not found in schema");
        ex.Message.ShouldContain("target key member");
        ex.Message.ShouldContain("EntityWithValidFk");
    }

    [Fact]
    public void Model_build_is_deterministic_and_reusable()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var context1 = new TestDb2Context(dbdProvider, db2Provider);
        var context2 = new TestDb2Context(dbdProvider, db2Provider);

        var mapEntity1 = context1.Model.GetEntityType(typeof(Fixtures.Map));
        var mapEntity2 = context2.Model.GetEntityType(typeof(Fixtures.Map));

        mapEntity1.TableName.ShouldBe(mapEntity2.TableName);
        mapEntity1.PrimaryKeyMember.Name.ShouldBe(mapEntity2.PrimaryKeyMember.Name);
        mapEntity1.PrimaryKeyFieldSchema.Name.ShouldBe(mapEntity2.PrimaryKeyFieldSchema.Name);
        mapEntity1.PrimaryKeyFieldSchema.ColumnStartIndex.ShouldBe(mapEntity2.PrimaryKeyFieldSchema.ColumnStartIndex);

        context1.Model.TryGetReferenceNavigation(typeof(Fixtures.Map), typeof(Fixtures.Map).GetProperty(nameof(Fixtures.Map.ParentMap))!, out var nav1).ShouldBeTrue();
        context2.Model.TryGetReferenceNavigation(typeof(Fixtures.Map), typeof(Fixtures.Map).GetProperty(nameof(Fixtures.Map.ParentMap))!, out var nav2).ShouldBeTrue();

        nav1.SourceKeyFieldSchema.Name.ShouldBe(nav2.SourceKeyFieldSchema.Name);
        nav1.TargetKeyFieldSchema.Name.ShouldBe(nav2.TargetKeyFieldSchema.Name);
    }

    [Fact]
    public void Model_exposes_resolved_field_schemas_for_query_planning()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var mapEntity = context.Model.GetEntityType(typeof(Fixtures.Map));

        mapEntity.PrimaryKeyFieldSchema.Name.ShouldNotBeNullOrWhiteSpace();
        mapEntity.PrimaryKeyFieldSchema.IsId.ShouldBeTrue();

        context.Model.TryGetReferenceNavigation(typeof(Fixtures.Map), typeof(Fixtures.Map).GetProperty(nameof(Fixtures.Map.ParentMap))!, out var nav).ShouldBeTrue();

        nav.SourceKeyFieldSchema.Name.ShouldNotBeNullOrWhiteSpace();
        nav.TargetKeyFieldSchema.Name.ShouldNotBeNullOrWhiteSpace();

        nav.SourceKeyFieldSchema.Name.ShouldBe("ParentMapID", StringComparer.OrdinalIgnoreCase);
        nav.TargetKeyFieldSchema.Name.ShouldBe("ID", StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public void HasPrincipalKey_configures_navigation_with_custom_target_key()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new HasPrincipalKeySuccessTestContext(dbdProvider, db2Provider);

        // Verify the navigation was configured successfully
        context.Model.TryGetReferenceNavigation(
            typeof(EntityWithCustomPrincipalKey),
            typeof(EntityWithCustomPrincipalKey).GetProperty(nameof(EntityWithCustomPrincipalKey.Target))!,
            out var nav).ShouldBeTrue();

        // Verify it uses the custom principal key field (Id from Map)
        nav.SourceKeyFieldSchema.Name.ShouldBe("ParentMapID", StringComparer.OrdinalIgnoreCase);
        nav.TargetKeyFieldSchema.Name.ShouldBe("ID", StringComparer.OrdinalIgnoreCase);
        nav.TargetKeyFieldSchema.IsId.ShouldBeTrue();
    }
}

internal sealed class MissingPkFieldInSchemaTestContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider)
{
    public Db2Table<EntityWithNonExistentPkField> Test { get; init; } = null!;

    protected override void OnModelCreating(Db2ModelBuilder modelBuilder)
        => modelBuilder
            .Entity<EntityWithNonExistentPkField>()
            .ToTable("Map")
            .HasKey(x => x.NonExistentKey);
}

internal sealed class MissingFkFieldInSchemaTestContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider)
{
    public Db2Table<Map> Map { get; init; } = null!;
    public Db2Table<EntityWithNonExistentFkField> Test { get; init; } = null!;

    protected override void OnModelCreating(Db2ModelBuilder modelBuilder)
        => modelBuilder
            .Entity<EntityWithNonExistentFkField>()
            .ToTable("Map")
            .HasOne(x => x.Target)
            .WithForeignKey(x => x.NonExistentForeignKey);
}

internal sealed class MissingTargetPkFieldInSchemaTestContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider)
{
    public Db2Table<EntityWithValidFk> Test { get; init; } = null!;

    protected override void OnModelCreating(Db2ModelBuilder modelBuilder)
    {
        modelBuilder
            .Entity<MapTargetWithFakeKey>()
            .ToTable("Map");

        modelBuilder
            .Entity<EntityWithValidFk>()
            .ToTable("Map")
            .HasOne(x => x.Target)
            .WithForeignKey(x => x.ParentMapID)
            .HasPrincipalKey(x => x.FakeTargetKey);
    }
}

internal sealed class EntityWithNonExistentPkField
{
    public int NonExistentKey { get; init; }
}

internal sealed class EntityWithNonExistentFkField
{
    public int Id { get; init; }
    public int NonExistentForeignKey { get; init; }
    public Map? Target { get; init; }
}

internal sealed class EntityWithValidFk
{
    public int Id { get; init; }
    public int ParentMapID { get; init; }
    public MapTargetWithFakeKey? Target { get; init; }
}

internal sealed class MapTargetWithFakeKey
{
    public int Id { get; init; }
    public int FakeTargetKey { get; init; }
    public string Directory { get; init; } = string.Empty;
}

internal sealed class HasPrincipalKeySuccessTestContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider)
{
    public Db2Table<EntityWithCustomPrincipalKey> Test { get; init; } = null!;

    protected override void OnModelCreating(Db2ModelBuilder modelBuilder)
    {
        modelBuilder
            .Entity<Map>()
            .ToTable("Map");

        modelBuilder
            .Entity<EntityWithCustomPrincipalKey>()
            .ToTable("Map")
            .HasOne(x => x.Target)
            .WithForeignKey(x => x.ParentMapID)
            .HasPrincipalKey(x => x.Id);
    }
}

internal sealed class EntityWithCustomPrincipalKey
{
    public int Id { get; init; }
    public int ParentMapID { get; init; }
    public Map? Target { get; init; }
}
