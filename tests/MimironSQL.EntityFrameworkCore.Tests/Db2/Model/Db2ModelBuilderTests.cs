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
}
