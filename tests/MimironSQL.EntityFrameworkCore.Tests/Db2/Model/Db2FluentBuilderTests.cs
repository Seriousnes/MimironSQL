using System.ComponentModel.DataAnnotations.Schema;

using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Schema;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2FluentBuilderTests
{
    [Fact]
    public void Fluent_reference_navigation_WithForeignKey_resolves_target_primary_key_by_default()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<Child>()
            .HasOne(x => x.Parent)
            .WithForeignKey(x => x.ParentId)
            .OverridesSchema();

        builder.Entity<Parent>().HasKey(x => x.Id);
        builder.Entity<Child>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolver);

        var navMember = typeof(Child).GetProperty(nameof(Child.Parent))!;
        model.TryGetReferenceNavigation(typeof(Child), navMember, out var nav).ShouldBeTrue();

        nav.Kind.ShouldBe(Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey);
        nav.SourceKeyMember.Name.ShouldBe(nameof(Child.ParentId));
        nav.TargetKeyMember.Name.ShouldBe(nameof(Parent.Id));
        nav.OverridesSchema.ShouldBeTrue();
    }

    [Fact]
    public void Fluent_reference_navigation_WithSharedPrimaryKey_sets_kind_and_keys()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<Child>()
            .HasOne(x => x.Parent)
            .WithSharedPrimaryKey(sourceKey: x => x.Id, targetKey: x => x.Id);

        builder.Entity<Parent>().HasKey(x => x.Id);
        builder.Entity<Child>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolver);

        var navMember = typeof(Child).GetProperty(nameof(Child.Parent))!;
        model.TryGetReferenceNavigation(typeof(Child), navMember, out var nav).ShouldBeTrue();

        nav.Kind.ShouldBe(Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne);
        nav.SourceKeyMember.Name.ShouldBe(nameof(Child.Id));
        nav.TargetKeyMember.Name.ShouldBe(nameof(Parent.Id));
    }

    [Fact]
    public void Fluent_collection_navigation_WithForeignKey_sets_dependent_fk_kind()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<Parent>()
            .HasMany(x => x.Children)
            .WithForeignKey(x => x.ParentId)
            .OverridesSchema();

        builder.Entity<Parent>().HasKey(x => x.Id);
        builder.Entity<Child>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolver);

        var navMember = typeof(Parent).GetProperty(nameof(Parent.Children))!;
        model.TryGetCollectionNavigation(typeof(Parent), navMember, out var nav).ShouldBeTrue();

        nav.Kind.ShouldBe(Db2CollectionNavigationKind.DependentForeignKeyToPrimaryKey);
        nav.DependentForeignKeyMember.ShouldNotBeNull();
        nav.DependentForeignKeyMember!.Name.ShouldBe(nameof(Child.ParentId));
        nav.PrincipalKeyMember.ShouldNotBeNull();
        nav.PrincipalKeyMember!.Name.ShouldBe(nameof(Parent.Id));
        nav.OverridesSchema.ShouldBeTrue();
    }

    [Fact]
    public void Fluent_collection_navigation_WithForeignKeyArray_sets_source_key_collection_kind()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<ParentWithChildIds>()
            .HasMany(x => x.Children)
            .WithForeignKeyArray(x => x.ChildIds);

        builder.Entity<ParentWithChildIds>().HasKey(x => x.Id);
        builder.Entity<Child>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolver);

        var navMember = typeof(ParentWithChildIds).GetProperty(nameof(ParentWithChildIds.Children))!;
        model.TryGetCollectionNavigation(typeof(ParentWithChildIds), navMember, out var nav).ShouldBeTrue();

        nav.Kind.ShouldBe(Db2CollectionNavigationKind.ForeignKeyArrayToPrimaryKey);
        nav.SourceKeyCollectionMember.ShouldNotBeNull();
        nav.SourceKeyCollectionMember!.Name.ShouldBe(nameof(ParentWithChildIds.ChildIds));
        nav.SourceKeyFieldSchema.ShouldNotBeNull();
    }

    [Fact]
    public void Property_HasColumnName_writes_mapping_into_entity_type()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<ColumnMapped>()
            .Property(x => x.DisplayName)
            .HasColumnName("Display_Name");

        builder.Entity<ColumnMapped>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolver);
        var entity = model.GetEntityType(typeof(ColumnMapped));

        entity.ColumnNameMappings[nameof(ColumnMapped.DisplayName)].ShouldBe("Display_Name");
    }

    [Fact]
    public void Property_HasColumnName_on_primary_key_throws()
    {
        var builder = new Db2ModelBuilder();

        var ex = Should.Throw<NotSupportedException>(() =>
            builder.Entity<ColumnMapped>()
                .Property(x => x.Id)
                .HasColumnName("Nope"));

        ex.Message.ShouldContain("Primary key property");
    }

    [Fact]
    public void Property_HasColumnName_when_ColumnAttribute_present_throws()
    {
        var builder = new Db2ModelBuilder();

        var ex = Should.Throw<NotSupportedException>(() =>
            builder.Entity<HasColumnAttribute>()
                .Property(x => x.Name)
                .HasColumnName("Other"));

        ex.Message.ShouldContain("[Column]");
    }

    private static Db2TableSchema SchemaResolver(string tableName)
    {
        return tableName switch
        {
            nameof(Parent) => new Db2TableSchema(
                tableName: nameof(Parent),
                layoutHash: 0,
                physicalColumnCount: 3,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Level), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Name), Db2ValueType.String, ColumnStartIndex: 2, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            nameof(Child) => new Db2TableSchema(
                tableName: nameof(Child),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Child.ParentId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(Parent)),
                ]),

            nameof(ParentWithChildIds) => new Db2TableSchema(
                tableName: nameof(ParentWithChildIds),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(ParentWithChildIds.ChildIds), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 3, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(Child)),
                ]),

            nameof(ColumnMapped) => new Db2TableSchema(
                tableName: nameof(ColumnMapped),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema("Display_Name", Db2ValueType.String, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            nameof(HasColumnAttribute) => new Db2TableSchema(
                tableName: nameof(HasColumnAttribute),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(HasColumnAttribute.Name), Db2ValueType.String, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private sealed class Parent
    {
        public int Id { get; set; }

        public int Level { get; set; }

        public string Name { get; set; } = string.Empty;

        public ICollection<Child> Children { get; set; } = [];
    }

    private sealed class Child
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public Parent? Parent { get; set; }
    }

    private sealed class ParentWithChildIds
    {
        public int Id { get; set; }

        public int[] ChildIds { get; set; } = [];

        public ICollection<Child> Children { get; set; } = [];
    }

    private sealed class ColumnMapped
    {
        public int Id { get; set; }

        public string DisplayName { get; set; } = string.Empty;
    }

    private sealed class HasColumnAttribute
    {
        public int Id { get; set; }

        [Column]
        public string Name { get; set; } = string.Empty;
    }
}
