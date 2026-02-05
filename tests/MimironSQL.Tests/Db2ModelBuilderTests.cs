using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;
using System.Reflection.Emit;

using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.Db2;

using Shouldly;

namespace MimironSQL.Tests;

public sealed class Db2ModelBuilderTests
{
    [Fact]
    public void Db2ModelBuilder_ApplyConfigurationsFromAssembly_DuplicateEntityConfigurations_ThrowsWithDetails()
    {
        var builder = new Db2ModelBuilder();

        var assembly = CreateConfigAssembly(
            new ConfigSpec("DuplicateConfigA", HasParameterlessConstructor: true, HasOnlyParameterizedConstructor: false),
            new ConfigSpec("DuplicateConfigB", HasParameterlessConstructor: true, HasOnlyParameterizedConstructor: false));

        var ex = Should.Throw<InvalidOperationException>(() => builder.ApplyConfigurationsFromAssembly(assembly));
        ex.Message.ShouldContain("Multiple entity type configurations found");
        ex.Message.ShouldContain("DuplicateConfigA");
        ex.Message.ShouldContain("DuplicateConfigB");
    }

    [Fact]
    public void Db2ModelBuilder_ApplyConfigurationsFromAssembly_CalledTwice_Throws()
    {
        var builder = new Db2ModelBuilder();
        builder.ApplyConfigurationsFromAssembly(typeof(Db2ModelBuilderTests).Assembly);

        var ex = Should.Throw<InvalidOperationException>(() => builder.ApplyConfigurationsFromAssembly(typeof(Db2ModelBuilderTests).Assembly));
        ex.Message.ShouldContain("can only be called once");
    }

    [Fact]
    public void Db2ModelBuilder_ApplyConfigurationsFromAssembly_NoPublicParameterlessCtor_ThrowsWithHint()
    {
        var builder = new Db2ModelBuilder();

        var assembly = CreateConfigAssembly(
            new ConfigSpec("NoDefaultCtorConfig", HasParameterlessConstructor: false, HasOnlyParameterizedConstructor: true));

        var ex = Should.Throw<InvalidOperationException>(() => builder.ApplyConfigurationsFromAssembly(assembly));
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

    private static Assembly CreateConfigAssembly(params ConfigSpec[] configSpecs)
    {
        var assemblyName = new AssemblyName($"MimironSQL.Tests.DynamicConfigs.{Guid.NewGuid():N}");
        var asm = AssemblyBuilder.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.Run);
        var module = asm.DefineDynamicModule(assemblyName.Name!);

        var entityType = typeof(Entity);
        var configInterface = typeof(IDb2EntityTypeConfiguration<>).MakeGenericType(entityType);
        var builderType = typeof(Db2EntityTypeBuilder<>).MakeGenericType(entityType);

        foreach (var spec in configSpecs)
        {
            var tb = module.DefineType(
                spec.TypeName,
                TypeAttributes.Public | TypeAttributes.Sealed | TypeAttributes.Class);

            tb.AddInterfaceImplementation(configInterface);

            if (spec.HasOnlyParameterizedConstructor)
            {
                var ctor = tb.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, [typeof(int)]);
                var il = ctor.GetILGenerator();
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Call, typeof(object).GetConstructor(Type.EmptyTypes)!);
                il.Emit(OpCodes.Ret);
            }
            else if (spec.HasParameterlessConstructor)
            {
                tb.DefineDefaultConstructor(MethodAttributes.Public);
            }

            var method = tb.DefineMethod(
                nameof(IDb2EntityTypeConfiguration<Entity>.Configure),
                MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.Final | MethodAttributes.HideBySig | MethodAttributes.NewSlot,
                CallingConventions.Standard,
                returnType: typeof(void),
                parameterTypes: [builderType]);

            method.GetILGenerator().Emit(OpCodes.Ret);

            tb.DefineMethodOverride(method, configInterface.GetMethod(nameof(IDb2EntityTypeConfiguration<Entity>.Configure))!);
            tb.CreateType();
        }

        return asm;
    }

    private readonly record struct ConfigSpec(string TypeName, bool HasParameterlessConstructor, bool HasOnlyParameterizedConstructor);

    public sealed class Entity
    {
        public int Id { get; set; }
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
}
