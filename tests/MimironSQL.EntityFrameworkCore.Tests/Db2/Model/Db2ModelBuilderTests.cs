using System.ComponentModel.DataAnnotations.Schema;

using MimironSQL.Db2;
using MimironSQL.Providers;

using Microsoft.Extensions.DependencyInjection;

using NSubstitute;

using Shouldly;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Schema;

using Microsoft.EntityFrameworkCore;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2ModelBindingTests
{
    [Fact]
    public void GetAutoIncludeNavigations_returns_eager_loaded_navigations_sorted_by_name()
    {
        var model = TestModelBindingFactory.CreateBinding(
            static modelBuilder =>
            {
                modelBuilder.Entity<AutoIncludeParent>().HasKey(x => x.Id);
                modelBuilder.Entity<AutoIncludeChild>().HasKey(x => x.Id);

                modelBuilder.Entity<AutoIncludeParent>()
                    .HasOne(x => x.AChild)
                    .WithMany()
                    .HasForeignKey(x => x.AChildId);

                modelBuilder.Entity<AutoIncludeParent>()
                    .HasMany(x => x.ZChildren)
                    .WithOne();

                modelBuilder.Entity<AutoIncludeParent>().Navigation(x => x.AChild).AutoInclude();
                modelBuilder.Entity<AutoIncludeParent>().Navigation(x => x.ZChildren).AutoInclude();
            },
            static _ => throw new InvalidOperationException("Schema resolver should not be invoked for this test."));

        var navs = model.GetAutoIncludeNavigations(typeof(AutoIncludeParent));

        navs.Select(static m => m.Name).ShouldBe([nameof(AutoIncludeParent.AChild), nameof(AutoIncludeParent.ZChildren)]);
    }

    [Fact]
    public void GetEntityType_FieldWithColumnAttribute_Throws()
    {
        var model = TestModelBindingFactory.CreateBinding(static _ => { }, static _ => throw new InvalidOperationException("Schema resolver should not be invoked for this test."));

        var ex = Should.Throw<NotSupportedException>(() => model.GetEntityType(typeof(EntityWithFieldColumnAttribute)));
        ex.Message.ShouldContain("Column mapping attributes are only supported on public properties");
        ex.Message.ShouldContain(nameof(EntityWithFieldColumnAttribute.Field));
    }

    [Fact]
    public void GetEntityType_NonPublicPropertyWithColumnAttribute_Throws()
    {
        var model = TestModelBindingFactory.CreateBinding(static _ => { }, static _ => throw new InvalidOperationException("Schema resolver should not be invoked for this test."));

        var ex = Should.Throw<NotSupportedException>(() => model.GetEntityType(typeof(EntityWithPrivateColumnProperty)));
        ex.Message.ShouldContain("Column mapping attributes are only supported on public properties");
        ex.Message.ShouldContain("Value");
    }

    [Fact]
    public void GetEntityType_NonPublicPropertyWithForeignKeyAttribute_Throws()
    {
        var model = TestModelBindingFactory.CreateBinding(static _ => { }, static _ => throw new InvalidOperationException("Schema resolver should not be invoked for this test."));

        var ex = Should.Throw<NotSupportedException>(() => model.GetEntityType(typeof(EntityWithPrivateForeignKeyProperty)));
        ex.Message.ShouldContain("Foreign key mapping attributes are only supported on public properties");
        ex.Message.ShouldContain("FooId");
    }

    [Fact]
    public void GetEntityType_when_entity_has_no_primary_key_throws()
    {
        var model = TestModelBindingFactory.CreateBinding(
            static modelBuilder =>
            {
                modelBuilder.Entity<NoKeyEntity>();
            },
            NoKeyEntitySchemaResolver);

        var ex = Should.Throw<NotSupportedException>(() => model.GetEntityType(typeof(NoKeyEntity)));
        ex.Message.ShouldContain("has no key member");
    }

    [Fact]
    public void GetEntityType_when_primary_key_member_is_not_public_property_throws()
    {
        var model = TestModelBindingFactory.CreateBinding(
            static modelBuilder =>
            {
                var entity = modelBuilder.Entity<FieldKeyEntity>();
                entity.Property<int>("Key");
                entity.HasKey("Key");
            },
            FieldKeyEntitySchemaResolver);

        var ex = Should.Throw<NotSupportedException>(() => model.GetEntityType(typeof(FieldKeyEntity)));
        ex.Message.ShouldContain("primary key must be a public property");
    }

    [Fact]
    public void GetEntityType_supports_primary_key_column_mapping_via_column_attribute()
    {
        var model = TestModelBindingFactory.CreateBinding(
            static modelBuilder =>
            {
                modelBuilder.Entity<ColumnMappedPrimaryKey>().HasKey(x => x.Id);
            },
            ColumnMappedPrimaryKeySchemaResolver);

        var entityType = model.GetEntityType(typeof(ColumnMappedPrimaryKey));
        entityType.PrimaryKeyFieldSchema.Name.ShouldBe("Other");
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
                    new Db2FieldSchema("Other", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                ]),
            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private sealed class EntityWithFieldColumnAttribute : Db2Entity<int>
    {
        [Column]
        public int Field;
    }

    private sealed class EntityWithPrivateColumnProperty : Db2Entity<int>
    {
        [Column]
        private int Value { get; set; }
    }

    private sealed class EntityWithPrivateForeignKeyProperty : Db2Entity<int>
    {
        [ForeignKey("Foo")]
        private int FooId { get; set; }

        public object Foo { get; set; } = new();
    }

    private sealed class NoKeyEntity : Db2Entity<int>
    {
        public int Key { get; set; }
    }

    private sealed class FieldKeyEntity : Db2Entity<int>
    {
    }

    private sealed class ColumnMappedPrimaryKey : Db2Entity<int>
    {
        [Column("Other")]
        public new int Id { get; set; }
    }

    [Table("MyTable")]
    private sealed class EntityWithTableAttribute : Db2Entity<int>
    {
    }

    private sealed class TableAttributeContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<EntityWithTableAttribute> Entities
        {
            get
            {
                return field ??= Set<EntityWithTableAttribute>();
            }
        }
    }

    private sealed class AutoIncludeParent : Db2Entity<int>
    {
        public int AChildId { get; set; }

        public AutoIncludeChild? AChild { get; set; }

        public ICollection<AutoIncludeChild> ZChildren { get; set; } = [];
    }

    private sealed class AutoIncludeChild : Db2Entity<int>
    {
    }
}
