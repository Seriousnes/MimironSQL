using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;

using MimironSQL.Db2.Model;
using MimironSQL.Db2;

using Shouldly;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Schema;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2ModelBuilderTests
{
    [Fact]
    public void Db2ModelBuilder_ApplyConfigurationsFromAssembly_DuplicateEntityConfigurations_ThrowsWithDetails()
    {
        var builder = new Db2ModelBuilder();

        // Use a dedicated helper assembly so this test doesn't conflict with other configuration-scanning tests.
        var assembly = typeof(MimironSQL.EntityFrameworkCore.TestConfigs.DuplicateConfigA).Assembly;

        var ex = Should.Throw<InvalidOperationException>(() => builder.ApplyConfigurationsFromAssembly(assembly));
        ex.Message.ShouldContain("Multiple entity type configurations found");
        ex.Message.ShouldContain("DuplicateConfigA");
        ex.Message.ShouldContain("DuplicateConfigB");
    }

    [Fact]
    public void Db2ModelBuilder_ApplyConfigurationsFromAssembly_CalledTwice_Throws()
    {
        var builder = new Db2ModelBuilder();
        // This assembly is guaranteed to contain no Db2 configuration types.
        builder.ApplyConfigurationsFromAssembly(typeof(string).Assembly);

        var ex = Should.Throw<InvalidOperationException>(() => builder.ApplyConfigurationsFromAssembly(typeof(string).Assembly));
        ex.Message.ShouldContain("can only be called once");
    }

    [Fact]
    public void Db2ModelBuilder_ApplyConfigurationsFromAssembly_NoPublicParameterlessCtor_ThrowsWithHint()
    {
        var builder = new Db2ModelBuilder();

        var ex = Should.Throw<InvalidOperationException>(() => builder.ApplyConfigurationsFromAssembly(typeof(NoDefaultCtorConfig).Assembly));
        ex.Message.ShouldContain("Unable to instantiate configuration type");
        ex.Message.ShouldContain("public parameterless constructor");
        ex.InnerException.ShouldNotBeNull();
    }

    [Fact]
    public void Db2ModelBuilder_Entity_FieldWithColumnAttribute_Throws()
    {
        var builder = new Db2ModelBuilder();

        var ex = Should.Throw<NotSupportedException>(() => builder.Entity<EntityWithFieldColumnAttribute>());
        ex.Message.ShouldContain("Column mapping attributes are only supported on public properties");
        ex.Message.ShouldContain(nameof(EntityWithFieldColumnAttribute.Field));
    }

    [Fact]
    public void Db2ModelBuilder_Entity_NonPublicPropertyWithColumnAttribute_Throws()
    {
        var builder = new Db2ModelBuilder();

        var ex = Should.Throw<NotSupportedException>(() => builder.Entity<EntityWithPrivateColumnProperty>());
        ex.Message.ShouldContain("Column mapping attributes are only supported on public properties");
        ex.Message.ShouldContain("Value");
    }

    [Fact]
    public void Db2ModelBuilder_Entity_NonPublicPropertyWithForeignKeyAttribute_Throws()
    {
        var builder = new Db2ModelBuilder();

        var ex = Should.Throw<NotSupportedException>(() => builder.Entity<EntityWithPrivateForeignKeyProperty>());
        ex.Message.ShouldContain("Foreign key mapping attributes are only supported on public properties");
        ex.Message.ShouldContain("FooId");
    }

    [Fact]
    public void Db2EntityTypeBuilder_ToTable_EntityHasTableAttribute_Throws()
    {
        var builder = new Db2ModelBuilder();
        var entity = builder.Entity<EntityWithTableAttribute>();

        var ex = Should.Throw<NotSupportedException>(() => entity.ToTable("Other"));
        ex.Message.ShouldContain("has a [Table] attribute");
    }

    [Fact]
    public void Db2ModelBuilder_Entity_TableAttribute_SetsTableNameAsConfigured()
    {
        var builder = new Db2ModelBuilder();
        var entity = builder.Entity<EntityWithTableAttribute>();

        entity.Metadata.TableName.ShouldBe("MyTable");
        entity.Metadata.TableNameWasConfigured.ShouldBeTrue();
    }

    [Fact]
    public void Db2ModelBuilder_ApplyAttributeNavigationConventions_ForeignKey_on_collection_can_target_source_key_array()
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<ArraySource>();
        builder.Entity<ArrayTarget>();

        builder.ApplyAttributeNavigationConventions();

        var model = builder.Build(SchemaResolver);

        var navMember = typeof(ArraySource).GetProperty(nameof(ArraySource.Targets))!;
        model.TryGetCollectionNavigation(typeof(ArraySource), navMember, out var nav).ShouldBeTrue();
        nav.Kind.ShouldBe(Db2CollectionNavigationKind.ForeignKeyArrayToPrimaryKey);
        nav.SourceKeyCollectionMember.ShouldNotBeNull();
        nav.SourceKeyCollectionMember!.Name.ShouldBe(nameof(ArraySource.TargetIds));
    }

    [Fact]
    public void Db2ModelBuilder_ApplyAttributeNavigationConventions_ForeignKey_on_scalar_fk_property_creates_reference_navigation()
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<ScalarFkSource>();
        builder.Entity<ScalarFkTarget>();

        builder.ApplyAttributeNavigationConventions();

        builder.Entity<ScalarFkSource>().HasKey(x => x.Id);
        builder.Entity<ScalarFkTarget>().HasKey(x => x.Id);

        var model = builder.Build(ScalarFkSchemaResolver);

        var navMember = typeof(ScalarFkSource).GetProperty(nameof(ScalarFkSource.Parent))!;
        model.TryGetReferenceNavigation(typeof(ScalarFkSource), navMember, out var nav).ShouldBeTrue();
        nav.Kind.ShouldBe(Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey);
        nav.SourceKeyMember.Name.ShouldBe(nameof(ScalarFkSource.ParentId));
        nav.TargetKeyMember.Name.ShouldBe(nameof(ScalarFkTarget.Id));
    }

    [Fact]
    public void Db2ModelBuilder_ApplyAttributeNavigationConventions_ForeignKey_with_composite_name_throws()
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<CompositeForeignKeyNameEntity>();

        var ex = Should.Throw<NotSupportedException>(() => builder.ApplyAttributeNavigationConventions());
        ex.Message.ShouldContain("does not support composite keys");
    }

    [Fact]
    public void Db2ModelBuilder_ApplyAttributeNavigationConventions_ForeignKey_on_collection_can_target_dependent_fk()
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<AttrParent>();
        builder.Entity<AttrChild>();

        builder.ApplyAttributeNavigationConventions();

        builder.Entity<AttrParent>().HasKey(x => x.Id);
        builder.Entity<AttrChild>().HasKey(x => x.Id);

        var model = builder.Build(AttrParentChildSchemaResolver);

        var navMember = typeof(AttrParent).GetProperty(nameof(AttrParent.Children))!;
        model.TryGetCollectionNavigation(typeof(AttrParent), navMember, out var nav).ShouldBeTrue();
        nav.Kind.ShouldBe(Db2CollectionNavigationKind.DependentForeignKeyToPrimaryKey);
        nav.DependentForeignKeyMember.ShouldNotBeNull();
        nav.DependentForeignKeyMember!.Name.ShouldBe(nameof(AttrChild.ParentId));
        nav.PrincipalKeyMember.ShouldNotBeNull();
        nav.PrincipalKeyMember!.Name.ShouldBe(nameof(AttrParent.Id));
    }

    [Fact]
    public void Db2ModelBuilder_ApplyAttributeNavigationConventions_ForeignKey_on_collection_missing_member_throws()
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<AttrParentMissingFk>();
        builder.Entity<AttrChild>();

        var ex = Should.Throw<NotSupportedException>(() => builder.ApplyAttributeNavigationConventions());
        ex.Message.ShouldContain("must reference either a dependent FK");
        ex.Message.ShouldContain("but no matching public property was found");
    }

    [Fact]
    public void Db2ModelBuilder_ApplySchemaNavigationConventions_creates_reference_navigation_and_updates_target_table_name()
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<SchemaConventionsChild>();

        builder.ApplySchemaNavigationConventions(SchemaConventionsSchemaResolver);

        builder.Entity<SchemaConventionsChild>().HasKey(x => x.Id);
        builder.Entity<SchemaConventionsParentEntity>().HasKey(x => x.Id);

        builder.Entity<SchemaConventionsParentEntity>().Metadata.TableName.ShouldBe("Parent");

        var model = builder.Build(SchemaConventionsSchemaResolver);

        var navMember = typeof(SchemaConventionsChild).GetProperty(nameof(SchemaConventionsChild.Parent))!;
        model.TryGetReferenceNavigation(typeof(SchemaConventionsChild), navMember, out var nav).ShouldBeTrue();
        nav.SourceKeyMember.Name.ShouldBe(nameof(SchemaConventionsChild.ParentId));
        nav.TargetClrType.ShouldBe(typeof(SchemaConventionsParentEntity));
    }

    [Fact]
    public void Db2ModelBuilder_ApplySchemaNavigationConventions_conflicting_non_fk_navigation_throws()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<SchemaConflictChild>()
            .HasOne(x => x.Parent)
            .WithSharedPrimaryKey(sourceKey: x => x.Id, targetKey: x => x.Id);

        builder.Entity<SchemaConflictParent>().HasKey(x => x.Id);
        builder.Entity<SchemaConflictChild>().HasKey(x => x.Id);

        var ex = Should.Throw<NotSupportedException>(() => builder.ApplySchemaNavigationConventions(SchemaConflictSchemaResolver));
        ex.Message.ShouldContain("conflicts with schema FK");
    }

    [Fact]
    public void Db2ModelBuilder_ApplySchemaNavigationConventions_fk_member_mismatch_throws()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<SchemaFkMismatchChild>()
            .HasOne(x => x.Parent)
            .WithForeignKey(x => x.OtherParentId);

        builder.Entity<SchemaFkMismatchParent>().HasKey(x => x.Id);
        builder.Entity<SchemaFkMismatchChild>().HasKey(x => x.Id);

        var ex = Should.Throw<NotSupportedException>(() => builder.ApplySchemaNavigationConventions(SchemaFkMismatchSchemaResolver));
        ex.Message.ShouldContain("has FK member");
        ex.Message.ShouldContain("schema FK is");
    }

    [Fact]
    public void Db2ModelBuilder_ApplySchemaNavigationConventions_when_OverridesSchema_does_not_throw_for_mismatch()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<SchemaFkMismatchChild>()
            .HasOne(x => x.Parent)
            .WithForeignKey(x => x.OtherParentId)
            .OverridesSchema();

        builder.Entity<SchemaFkMismatchParent>().HasKey(x => x.Id);
        builder.Entity<SchemaFkMismatchChild>().HasKey(x => x.Id);

        builder.ApplySchemaNavigationConventions(SchemaFkMismatchSchemaResolver);

        var model = builder.Build(SchemaFkMismatchSchemaResolver);
        var navMember = typeof(SchemaFkMismatchChild).GetProperty(nameof(SchemaFkMismatchChild.Parent))!;
        model.TryGetReferenceNavigation(typeof(SchemaFkMismatchChild), navMember, out var nav).ShouldBeTrue();
        nav.SourceKeyMember.Name.ShouldBe(nameof(SchemaFkMismatchChild.OtherParentId));
    }

    [Fact]
    public void Db2ModelBuilder_Build_when_entity_has_no_primary_key_throws()
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<NoKeyEntity>();
        var ex = Should.Throw<NotSupportedException>(() => builder.Build(NoKeyEntitySchemaResolver));
        ex.Message.ShouldContain("has no key member");
    }

    [Fact]
    public void Db2ModelBuilder_Build_when_primary_key_member_is_not_public_property_throws()
    {
        var builder = new Db2ModelBuilder();
        var entity = builder.Entity<FieldKeyEntity>();
        entity.Metadata.PrimaryKeyMember = typeof(FieldKeyEntity).GetField(nameof(FieldKeyEntity.Id), BindingFlags.Instance | BindingFlags.Public)!;

        var ex = Should.Throw<NotSupportedException>(() => builder.Build(FieldKeyEntitySchemaResolver));
        ex.Message.ShouldContain("must be a public property");
    }

    [Fact]
    public void Db2ModelBuilder_Build_when_primary_key_has_column_attribute_throws()
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<ColumnMappedPrimaryKey>().HasKey(x => x.Id);

        var ex = Should.Throw<NotSupportedException>(() => builder.Build(ColumnMappedPrimaryKeySchemaResolver));
        ex.Message.ShouldContain("Primary key member");
        ex.Message.ShouldContain("cannot configure column mapping");
    }

    [Fact]
    public void Db2ModelBuilder_Build_when_foreign_key_array_is_not_integer_key_collection_throws()
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<BadKeyArraySource>();
        builder.Entity<BadKeyArrayTarget>();

        builder.ApplyAttributeNavigationConventions();

        builder.Entity<BadKeyArraySource>().HasKey(x => x.Id);
        builder.Entity<BadKeyArrayTarget>().HasKey(x => x.Id);

        var ex = Should.Throw<NotSupportedException>(() => builder.Build(BadKeyArraySchemaResolver));
        ex.Message.ShouldContain("expects an integer key collection");
    }

    private static Db2TableSchema SchemaResolver(string tableName)
    {
        return tableName switch
        {
            nameof(ArraySource) => new Db2TableSchema(
                tableName: nameof(ArraySource),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(ArraySource.TargetIds), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 3, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(ArrayTarget)),
                ]),
            nameof(ArrayTarget) => new Db2TableSchema(
                tableName: nameof(ArrayTarget),
                layoutHash: 0,
                physicalColumnCount: 1,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                ]),
            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private static Db2TableSchema ScalarFkSchemaResolver(string tableName)
    {
        return tableName switch
        {
            nameof(ScalarFkSource) => new Db2TableSchema(
                tableName: nameof(ScalarFkSource),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(ScalarFkSource.ParentId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(ScalarFkTarget)),
                ]),
            nameof(ScalarFkTarget) => new Db2TableSchema(
                tableName: nameof(ScalarFkTarget),
                layoutHash: 0,
                physicalColumnCount: 1,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                ]),
            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private static Db2TableSchema AttrParentChildSchemaResolver(string tableName)
    {
        return tableName switch
        {
            nameof(AttrParent) => new Db2TableSchema(
                tableName: nameof(AttrParent),
                layoutHash: 0,
                physicalColumnCount: 1,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                ]),
            nameof(AttrParentMissingFk) => new Db2TableSchema(
                tableName: nameof(AttrParentMissingFk),
                layoutHash: 0,
                physicalColumnCount: 1,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                ]),
            nameof(AttrChild) => new Db2TableSchema(
                tableName: nameof(AttrChild),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(AttrChild.ParentId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(AttrParent)),
                ]),
            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private static Db2TableSchema SchemaConventionsSchemaResolver(string tableName)
    {
        return tableName switch
        {
            nameof(SchemaConventionsChild) => new Db2TableSchema(
                tableName: nameof(SchemaConventionsChild),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(SchemaConventionsChild.ParentId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: "Parent"),
                ]),
            "Parent" => new Db2TableSchema(
                tableName: "Parent",
                layoutHash: 0,
                physicalColumnCount: 1,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                ]),
            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private static Db2TableSchema SchemaConflictSchemaResolver(string tableName)
    {
        return tableName switch
        {
            nameof(SchemaConflictChild) => new Db2TableSchema(
                tableName: nameof(SchemaConflictChild),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(SchemaConflictChild.ParentId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(SchemaConflictParent)),
                ]),
            nameof(SchemaConflictParent) => new Db2TableSchema(
                tableName: nameof(SchemaConflictParent),
                layoutHash: 0,
                physicalColumnCount: 1,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                ]),
            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private static Db2TableSchema SchemaFkMismatchSchemaResolver(string tableName)
    {
        return tableName switch
        {
            nameof(SchemaFkMismatchChild) => new Db2TableSchema(
                tableName: nameof(SchemaFkMismatchChild),
                layoutHash: 0,
                physicalColumnCount: 3,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(SchemaFkMismatchChild.ParentId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(SchemaFkMismatchParent)),
                    new Db2FieldSchema(nameof(SchemaFkMismatchChild.OtherParentId), Db2ValueType.Int64, ColumnStartIndex: 2, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(SchemaFkMismatchParent)),
                ]),
            nameof(SchemaFkMismatchParent) => new Db2TableSchema(
                tableName: nameof(SchemaFkMismatchParent),
                layoutHash: 0,
                physicalColumnCount: 1,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                ]),
            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private static Db2TableSchema NoKeyEntitySchemaResolver(string tableName)
    {
        return tableName switch
        {
            nameof(NoKeyEntity) => new Db2TableSchema(
                tableName: nameof(NoKeyEntity),
                layoutHash: 0,
                physicalColumnCount: 1,
                fields:
                [
                    new Db2FieldSchema("Key", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),
            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private static Db2TableSchema FieldKeyEntitySchemaResolver(string tableName)
    {
        return tableName switch
        {
            nameof(FieldKeyEntity) => new Db2TableSchema(
                tableName: nameof(FieldKeyEntity),
                layoutHash: 0,
                physicalColumnCount: 1,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                ]),
            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private static Db2TableSchema ColumnMappedPrimaryKeySchemaResolver(string tableName)
    {
        return tableName switch
        {
            nameof(ColumnMappedPrimaryKey) => new Db2TableSchema(
                tableName: nameof(ColumnMappedPrimaryKey),
                layoutHash: 0,
                physicalColumnCount: 1,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                ]),
            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private static Db2TableSchema BadKeyArraySchemaResolver(string tableName)
    {
        return tableName switch
        {
            nameof(BadKeyArraySource) => new Db2TableSchema(
                tableName: nameof(BadKeyArraySource),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(BadKeyArraySource.TargetIds), Db2ValueType.String, ColumnStartIndex: 1, ElementCount: 3, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(BadKeyArrayTarget)),
                ]),
            nameof(BadKeyArrayTarget) => new Db2TableSchema(
                tableName: nameof(BadKeyArrayTarget),
                layoutHash: 0,
                physicalColumnCount: 1,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                ]),
            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    public sealed class NoDefaultCtorEntity
    {
        public int Id { get; set; }
    }

    public sealed class NoDefaultCtorConfig(int _)
        : IDb2EntityTypeConfiguration<NoDefaultCtorEntity>
    {
        void IDb2EntityTypeConfiguration<NoDefaultCtorEntity>.Configure(Db2EntityTypeBuilder<NoDefaultCtorEntity> builder)
            => builder.HasKey(x => x.Id);
    }

    private sealed class EntityWithFieldColumnAttribute
    {
        [Column]
        public int Field;
    }

    private sealed class EntityWithPrivateColumnProperty
    {
        [Column]
        private int Value { get; set; }
    }

    private sealed class EntityWithPrivateForeignKeyProperty
    {
        [ForeignKey("Foo")]
        private int FooId { get; set; }

        public object Foo { get; set; } = new();
    }

    [Table("MyTable")]
    private sealed class EntityWithTableAttribute
    {
        public int Id { get; set; }
    }

    private sealed class ArraySource
    {
        public int Id { get; set; }

        public ICollection<ushort> TargetIds { get; set; } = [];

        [ForeignKey(nameof(TargetIds))]
        public ICollection<ArrayTarget> Targets { get; set; } = [];
    }

    private sealed class ArrayTarget
    {
        public int Id { get; set; }
    }

    private sealed class ScalarFkSource
    {
        public int Id { get; set; }

        [ForeignKey(nameof(Parent))]
        public int ParentId { get; set; }

        public ScalarFkTarget? Parent { get; set; }
    }

    private sealed class ScalarFkTarget
    {
        public int Id { get; set; }
    }

    private sealed class CompositeForeignKeyNameEntity
    {
        public int Id { get; set; }

        [ForeignKey("A, B")]
        public ScalarFkTarget? Parent { get; set; }
    }

    private sealed class AttrParent
    {
        public int Id { get; set; }

        [ForeignKey(nameof(AttrChild.ParentId))]
        public ICollection<AttrChild> Children { get; set; } = [];
    }

    private sealed class AttrParentMissingFk
    {
        public int Id { get; set; }

        [ForeignKey("Missing")]
        public ICollection<AttrChild> Children { get; set; } = [];
    }

    private sealed class AttrChild
    {
        public int Id { get; set; }

        public int ParentId { get; set; }
    }

    private sealed class SchemaConventionsChild
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public SchemaConventionsParentEntity? Parent { get; set; }
    }

    private sealed class SchemaConventionsParentEntity
    {
        public int Id { get; set; }
    }

    private sealed class SchemaConflictParent
    {
        public int Id { get; set; }
    }

    private sealed class SchemaConflictChild
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public SchemaConflictParent? Parent { get; set; }
    }

    private sealed class SchemaFkMismatchParent
    {
        public int Id { get; set; }
    }

    private sealed class SchemaFkMismatchChild
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public int OtherParentId { get; set; }

        public SchemaFkMismatchParent? Parent { get; set; }
    }

    private sealed class NoKeyEntity
    {
        public int Key { get; set; }
    }

    private sealed class FieldKeyEntity
    {
        public int Id;
    }

    private sealed class ColumnMappedPrimaryKey
    {
        [Column("Other")]
        public int Id { get; set; }
    }

    private sealed class BadKeyArraySource
    {
        public int Id { get; set; }

        public string[] TargetIds { get; set; } = [];

        [ForeignKey(nameof(TargetIds))]
        public ICollection<BadKeyArrayTarget> Targets { get; set; } = [];
    }

    private sealed class BadKeyArrayTarget
    {
        public int Id { get; set; }
    }
}
