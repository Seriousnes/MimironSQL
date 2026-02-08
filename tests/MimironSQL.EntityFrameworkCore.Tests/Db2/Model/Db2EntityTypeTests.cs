using System.ComponentModel.DataAnnotations.Schema;

using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.EntityFrameworkCore.Db2.Schema;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2EntityTypeTests
{
    private sealed class Entity
    {
        public int Id { get; init; }

        public int Level { get; init; }

        [Column("NAME")]
        public string Name { get; init; } = string.Empty;

        public int Missing { get; init; }

        public int Hidden { private get; init; }
    }

    [Fact]
    public void TryResolveFieldSchema_primary_key_member_is_resolved_without_schema_lookup()
    {
        var idMember = typeof(Entity).GetProperty(nameof(Entity.Id))!;

        var pkField = new Db2FieldSchema(
            Name: "ID",
            ValueType: Db2ValueType.Int64,
            ColumnStartIndex: 0,
            ElementCount: 1,
            IsVerified: true,
            IsVirtual: false,
            IsId: true,
            IsRelation: false,
            ReferencedTableName: null);

        var schema = new Db2TableSchema(
            tableName: "Entity",
            layoutHash: 0,
            physicalColumnCount: 1,
            fields: [pkField]);

        var entityType = new Db2EntityType(
            clrType: typeof(Entity),
            tableName: "Entity",
            schema: schema,
            primaryKeyMember: idMember,
            primaryKeyFieldSchema: pkField,
            columnNameMappings: new Dictionary<string, string>());

        entityType.TryResolveFieldSchema(idMember, out var resolved).ShouldBeTrue();
        resolved.ShouldBe(pkField);
    }

    [Fact]
    public void TryResolveFieldSchema_uses_configured_column_name_mapping()
    {
        var idMember = typeof(Entity).GetProperty(nameof(Entity.Id))!;
        var levelMember = typeof(Entity).GetProperty(nameof(Entity.Level))!;

        var idField = new Db2FieldSchema("ID", Db2ValueType.Int64, 0, 1, true, false, true, false, null);
        var lvlField = new Db2FieldSchema("Lvl", Db2ValueType.Int64, 1, 1, true, false, false, false, null);

        var schema = new Db2TableSchema("Entity", 0, 2, [idField, lvlField]);

        var entityType = new Db2EntityType(
            clrType: typeof(Entity),
            tableName: "Entity",
            schema: schema,
            primaryKeyMember: idMember,
            primaryKeyFieldSchema: idField,
            columnNameMappings: new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [nameof(Entity.Level)] = "Lvl",
            });

        entityType.TryResolveFieldSchema(levelMember, out var resolved).ShouldBeTrue();
        resolved.ShouldBe(lvlField);
    }

    [Fact]
    public void TryResolveFieldSchema_uses_Column_attribute_name_when_present()
    {
        var idMember = typeof(Entity).GetProperty(nameof(Entity.Id))!;
        var nameMember = typeof(Entity).GetProperty(nameof(Entity.Name))!;

        var idField = new Db2FieldSchema("ID", Db2ValueType.Int64, 0, 1, true, false, true, false, null);
        var nameField = new Db2FieldSchema("NAME", Db2ValueType.String, 1, 1, true, false, false, false, null);

        var schema = new Db2TableSchema("Entity", 0, 2, [idField, nameField]);

        var entityType = new Db2EntityType(
            clrType: typeof(Entity),
            tableName: "Entity",
            schema: schema,
            primaryKeyMember: idMember,
            primaryKeyFieldSchema: idField,
            columnNameMappings: new Dictionary<string, string>());

        entityType.TryResolveFieldSchema(nameMember, out var resolved).ShouldBeTrue();
        resolved.ShouldBe(nameField);
    }

    [Fact]
    public void TryResolveFieldSchema_returns_false_for_property_with_non_public_getter()
    {
        var idMember = typeof(Entity).GetProperty(nameof(Entity.Id))!;
        var hiddenMember = typeof(Entity).GetProperty(nameof(Entity.Hidden))!;

        hiddenMember.GetMethod.ShouldNotBeNull();
        hiddenMember.GetMethod!.IsPublic.ShouldBeFalse();

        var idField = new Db2FieldSchema("ID", Db2ValueType.Int64, 0, 1, true, false, true, false, null);
        var schema = new Db2TableSchema("Entity", 0, 1, [idField]);

        var entityType = new Db2EntityType(
            clrType: typeof(Entity),
            tableName: "Entity",
            schema: schema,
            primaryKeyMember: idMember,
            primaryKeyFieldSchema: idField,
            columnNameMappings: new Dictionary<string, string>());

        entityType.TryResolveFieldSchema(hiddenMember, out _).ShouldBeFalse();
    }

    [Fact]
    public void ResolveFieldSchema_throws_when_mapping_cannot_be_resolved()
    {
        var idMember = typeof(Entity).GetProperty(nameof(Entity.Id))!;
        var missingMember = typeof(Entity).GetProperty(nameof(Entity.Missing))!;

        var idField = new Db2FieldSchema("ID", Db2ValueType.Int64, 0, 1, true, false, true, false, null);
        var schema = new Db2TableSchema("Entity", 0, 1, [idField]);

        var entityType = new Db2EntityType(
            clrType: typeof(Entity),
            tableName: "Entity",
            schema: schema,
            primaryKeyMember: idMember,
            primaryKeyFieldSchema: idField,
            columnNameMappings: new Dictionary<string, string>());

        Should.Throw<NotSupportedException>(() => entityType.ResolveFieldSchema(missingMember, context: "unit-test"));
    }

    [Fact]
    public void WithSchema_throws_when_id_field_is_missing()
    {
        var idMember = typeof(Entity).GetProperty(nameof(Entity.Id))!;

        var schemaWithoutId = new Db2TableSchema(
            tableName: "NoId",
            layoutHash: 0,
            physicalColumnCount: 1,
            fields:
            [
                new Db2FieldSchema("Level", Db2ValueType.Int64, 0, 1, true, false, false, false, null),
            ]);

        var original = new Db2EntityType(
            clrType: typeof(Entity),
            tableName: "Entity",
            schema: schemaWithoutId,
            primaryKeyMember: idMember,
            primaryKeyFieldSchema: new Db2FieldSchema("ID", Db2ValueType.Int64, 0, 1, true, false, true, false, null),
            columnNameMappings: new Dictionary<string, string>());

        Should.Throw<NotSupportedException>(() => original.WithSchema("NoId", schemaWithoutId));
    }

    [Fact]
    public void WithSchema_updates_table_name_and_primary_key_field_schema()
    {
        var idMember = typeof(Entity).GetProperty(nameof(Entity.Id))!;

        var originalId = new Db2FieldSchema("ID", Db2ValueType.Int64, 0, 1, true, false, true, false, null);
        var originalSchema = new Db2TableSchema("Entity", 0, 1, [originalId]);

        var updatedId = new Db2FieldSchema("NewId", Db2ValueType.Int64, 0, 1, true, false, true, false, null);
        var updatedSchema = new Db2TableSchema("EntityRenamed", 0, 1, [updatedId]);

        var original = new Db2EntityType(
            clrType: typeof(Entity),
            tableName: "Entity",
            schema: originalSchema,
            primaryKeyMember: idMember,
            primaryKeyFieldSchema: originalId,
            columnNameMappings: new Dictionary<string, string>());

        var updated = original.WithSchema(tableName: "EntityRenamed", schema: updatedSchema);

        updated.TableName.ShouldBe("EntityRenamed");
        updated.PrimaryKeyMember.ShouldBe(idMember);
        updated.PrimaryKeyFieldSchema.ShouldBe(updatedId);
    }

    [Fact]
    public void NavigationJoinPlan_shared_primary_key_one_to_one_uses_entity_primary_keys()
    {
        var idMember = typeof(Entity).GetProperty(nameof(Entity.Id))!;

        var rootPk = new Db2FieldSchema("ID", Db2ValueType.Int64, 0, 1, true, false, true, false, null);
        var targetPk = new Db2FieldSchema("ID", Db2ValueType.Int64, 0, 1, true, false, true, false, null);

        var rootSchema = new Db2TableSchema("Root", 0, 1, [rootPk]);
        var targetSchema = new Db2TableSchema("Target", 0, 1, [targetPk]);

        var root = new Db2EntityType(typeof(Entity), "Root", rootSchema, idMember, rootPk, new Dictionary<string, string>());
        var target = new Db2EntityType(typeof(Entity), "Target", targetSchema, idMember, targetPk, new Dictionary<string, string>());

        var nav = new Db2ReferenceNavigation(
            sourceClrType: typeof(Entity),
            navigationMember: idMember,
            targetClrType: typeof(Entity),
            kind: Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne,
            sourceKeyMember: typeof(Entity).GetProperty(nameof(Entity.Level))!,
            targetKeyMember: typeof(Entity).GetProperty(nameof(Entity.Level))!,
            sourceKeyFieldSchema: new Db2FieldSchema("Level", Db2ValueType.Int64, 1, 1, true, false, false, false, null),
            targetKeyFieldSchema: new Db2FieldSchema("Level", Db2ValueType.Int64, 1, 1, true, false, false, false, null),
            overridesSchema: false);

        var plan = new Db2NavigationJoinPlan(root, nav, target);

        plan.RootKeyMember.ShouldBe(root.PrimaryKeyMember);
        plan.TargetKeyMember.ShouldBe(target.PrimaryKeyMember);
        plan.RootKeyFieldSchema.ShouldBe(root.PrimaryKeyFieldSchema);
        plan.TargetKeyFieldSchema.ShouldBe(target.PrimaryKeyFieldSchema);
    }

    [Fact]
    public void NavigationJoinPlan_unsupported_navigation_kind_throws()
    {
        var idMember = typeof(Entity).GetProperty(nameof(Entity.Id))!;
        var pk = new Db2FieldSchema("ID", Db2ValueType.Int64, 0, 1, true, false, true, false, null);
        var schema = new Db2TableSchema("Entity", 0, 1, [pk]);

        var entity = new Db2EntityType(typeof(Entity), "Entity", schema, idMember, pk, new Dictionary<string, string>());

        var nav = new Db2ReferenceNavigation(
            sourceClrType: typeof(Entity),
            navigationMember: idMember,
            targetClrType: typeof(Entity),
            kind: (Db2ReferenceNavigationKind)123,
            sourceKeyMember: idMember,
            targetKeyMember: idMember,
            sourceKeyFieldSchema: pk,
            targetKeyFieldSchema: pk,
            overridesSchema: false);

        var plan = new Db2NavigationJoinPlan(entity, nav, entity);

        Should.Throw<NotSupportedException>(() => _ = plan.RootKeyMember);
        Should.Throw<NotSupportedException>(() => _ = plan.TargetKeyMember);
        Should.Throw<NotSupportedException>(() => _ = plan.RootKeyFieldSchema);
        Should.Throw<NotSupportedException>(() => _ = plan.TargetKeyFieldSchema);
    }
}
