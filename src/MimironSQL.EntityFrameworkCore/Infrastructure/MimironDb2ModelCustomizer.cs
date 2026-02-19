using System.Reflection;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

using MimironSQL.EntityFrameworkCore.Extensions;
using MimironSQL.EntityFrameworkCore.Model;

namespace MimironSQL.EntityFrameworkCore.Infrastructure;

internal sealed class MimironDb2ModelCustomizer(ModelCustomizerDependencies dependencies) : ModelCustomizer(dependencies)
{
    public override void Customize(ModelBuilder modelBuilder, DbContext context)
    {
        base.Customize(modelBuilder, context);

        var options = context.GetService<IDbContextOptions>();
        var extension = options.FindExtension<MimironDb2OptionsExtension>();
        var modeling = extension?.ForeignKeyArrayModeling ?? ForeignKeyArrayModeling.SharedTypeJoinEntity;

        Db2ForeignKeyArrayModelRewriter.Rewrite(modelBuilder, modeling);
    }
}

internal static class Db2ForeignKeyArrayModelRewriter
{
    internal const string VirtualForeignKeyArrayPropertyAnnotation = "MimironDb2:VirtualForeignKeyArrayProperty";
    internal const string VirtualForeignKeyArrayNullSentinelAnnotation = "MimironDb2:VirtualForeignKeyArrayNullSentinel";

    public static void Rewrite(ModelBuilder modelBuilder, ForeignKeyArrayModeling modeling)
    {
        // Snapshot the entity types since we mutate the model.
        var entityTypes = modelBuilder.Model.GetEntityTypes().ToArray();
        foreach (var entityType in entityTypes)
        {
            var navigations = entityType.GetNavigations().ToArray();
            foreach (var navigation in navigations)
            {
                if (!Db2ForeignKeyArrayAnnotations.TryGetForeignKeyArrayPropertyName(navigation, out var foreignKeyArrayPropertyName))
                    continue;

                if (!navigation.IsCollection)
                    continue;

                RewriteOne(modelBuilder, entityType, navigation, foreignKeyArrayPropertyName, modeling);
            }
        }
    }

    private static void RewriteOne(
        ModelBuilder modelBuilder,
        IMutableEntityType principalEntityType,
        IMutableNavigation navigation,
        string foreignKeyArrayPropertyName,
        ForeignKeyArrayModeling modeling)
    {
        var principalClrType = principalEntityType.ClrType;
        var targetClrType = navigation.TargetEntityType.ClrType;

        var principalPk = principalEntityType.FindPrimaryKey();
        var targetPk = navigation.TargetEntityType.FindPrimaryKey();

        if (principalPk is null || principalPk.Properties.Count != 1)
            throw new NotSupportedException($"Entity '{principalEntityType.DisplayName()}' must have a single-column primary key to use HasForeignKeyArray.");
        if (targetPk is null || targetPk.Properties.Count != 1)
            throw new NotSupportedException($"Entity '{navigation.TargetEntityType.DisplayName()}' must have a single-column primary key to use HasForeignKeyArray.");

        var principalKeyType = principalPk.Properties[0].ClrType;
        var targetKeyType = targetPk.Properties[0].ClrType;

        var fkArrayProperty = principalClrType.GetProperty(foreignKeyArrayPropertyName, BindingFlags.Instance | BindingFlags.Public)
            ?? throw new NotSupportedException($"FK array property '{principalClrType.FullName}.{foreignKeyArrayPropertyName}' was not found.");

        if (!TryGetIEnumerableElementType(fkArrayProperty.PropertyType, out var elementType) || elementType is null)
            throw new NotSupportedException($"FK array property '{principalClrType.FullName}.{foreignKeyArrayPropertyName}' must be an IEnumerable<T>.");

        if (!IsIntegerLike(elementType))
            throw new NotSupportedException($"FK array property '{principalClrType.FullName}.{foreignKeyArrayPropertyName}' must be an integer-like element type.");

        if (!IsCompatibleIntegerLikeKey(elementType, targetKeyType))
            throw new NotSupportedException(
                $"FK array element type '{elementType.FullName}' does not match target PK type '{targetKeyType.FullName}' for navigation '{principalClrType.FullName}.{navigation.Name}'.");

        // Remove the original relationship so the navigation can be remapped as a skip navigation.
        var originalFk = navigation.ForeignKey;
        RemoveForeignKeyAndNavigations(originalFk);

        var joinName = GetJoinEntityName(principalEntityType, navigation);

        // Map to a many-to-many; join entity is virtual and executed by the provider later.
        var leftFkName = "LeftId";
        var rightFkName = "RightId";

        var cc = modelBuilder.Entity(principalClrType)
            .HasMany(targetClrType, navigation.Name)
            .WithMany();

        if (modeling == ForeignKeyArrayModeling.SharedTypeJoinEntity)
        {
            UsingEntityWithJoinType(
                cc,
                joinName,
                joinEntityType: typeof(Dictionary<string, object>),
                principalClrType,
                targetClrType,
                leftFkName,
                rightFkName,
                foreignKeyArrayPropertyName);
            return;
        }

        var joinClrType = typeof(Db2ForeignKeyArrayJoinRow<,>).MakeGenericType(principalKeyType.UnwrapNullable(), targetKeyType.UnwrapNullable());
        UsingEntityWithJoinType(
            cc,
            joinName,
            joinEntityType: joinClrType,
            principalClrType,
            targetClrType,
            leftFkName: nameof(Db2ForeignKeyArrayJoinRow<int, int>.LeftId),
            rightFkName: nameof(Db2ForeignKeyArrayJoinRow<int, int>.RightId),
            foreignKeyArrayPropertyName);
    }

    private static void UsingEntityWithJoinType(
        CollectionCollectionBuilder cc,
        string joinName,
        Type joinEntityType,
        Type principalClrType,
        Type targetClrType,
        string leftFkName,
        string rightFkName,
        string foreignKeyArrayPropertyName)
    {
        // Prefer overloads which accept (string joinEntityName, Type joinEntityType, ...) since join entity type is runtime.
        // EF Core has evolved overload sets; do reflection once and invoke.
        var methods = cc.GetType().GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Where(static m => m.Name == "UsingEntity")
            .ToArray();

        // Look for: UsingEntity(string joinEntityName, Type joinEntityType, Func<EntityTypeBuilder, ReferenceCollectionBuilder> configureRight, Func<EntityTypeBuilder, ReferenceCollectionBuilder> configureLeft, Action<EntityTypeBuilder> configureJoin)
        var candidate = methods.FirstOrDefault(m =>
        {
            var p = m.GetParameters();
            return p.Length == 5
                && p[0].ParameterType == typeof(string)
                && p[1].ParameterType == typeof(Type)
                && p[4].ParameterType == typeof(Action<Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder>);
        });

        if (candidate is null)
            throw new NotSupportedException("EF Core UsingEntity overload for CLR join entity was not found.");

        Microsoft.EntityFrameworkCore.Metadata.Builders.ReferenceCollectionBuilder ConfigureRight(Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder r)
            => r.HasOne(targetClrType).WithMany().HasForeignKey(rightFkName);

        Microsoft.EntityFrameworkCore.Metadata.Builders.ReferenceCollectionBuilder ConfigureLeft(Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder l)
            => l.HasOne(principalClrType).WithMany().HasForeignKey(leftFkName);

        void ConfigureJoin(Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder j)
        {
            j.HasKey(leftFkName, rightFkName);
            j.Metadata.SetAnnotation(VirtualForeignKeyArrayPropertyAnnotation, foreignKeyArrayPropertyName);
            j.Metadata.SetAnnotation(VirtualForeignKeyArrayNullSentinelAnnotation, 0L);
        }

        _ = candidate.Invoke(
            cc,
            [
                joinName,
                joinEntityType,
                (Func<Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder, Microsoft.EntityFrameworkCore.Metadata.Builders.ReferenceCollectionBuilder>)ConfigureRight,
                (Func<Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder, Microsoft.EntityFrameworkCore.Metadata.Builders.ReferenceCollectionBuilder>)ConfigureLeft,
                (Action<Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder>)ConfigureJoin,
            ]);
    }

    private static string GetJoinEntityName(IMutableEntityType principalEntityType, IMutableNavigation navigation)
        => principalEntityType.ShortName() + "__" + navigation.Name;

    private static void RemoveForeignKeyAndNavigations(IMutableForeignKey fk)
    {
        // Removing the FK removes its navigations as well.
        fk.DeclaringEntityType.RemoveForeignKey(fk);
    }

    private static bool IsIntegerLike(Type type)
    {
        type = type.UnwrapNullable();
        if (type.IsEnum)
            type = Enum.GetUnderlyingType(type);

        return type == typeof(byte)
            || type == typeof(sbyte)
            || type == typeof(short)
            || type == typeof(ushort)
            || type == typeof(int)
            || type == typeof(uint)
            || type == typeof(long)
            || type == typeof(ulong);
    }

    private static bool IsCompatibleIntegerLikeKey(Type elementType, Type targetKeyType)
    {
        elementType = elementType.UnwrapNullable();
        targetKeyType = targetKeyType.UnwrapNullable();

        if (elementType.IsEnum)
            elementType = Enum.GetUnderlyingType(elementType);
        if (targetKeyType.IsEnum)
            targetKeyType = Enum.GetUnderlyingType(targetKeyType);

        return elementType == targetKeyType;
    }

    private static bool TryGetIEnumerableElementType(Type type, out Type? elementType)
    {
        if (type == typeof(string))
        {
            elementType = null;
            return false;
        }

        if (type.IsArray)
        {
            elementType = type.GetElementType();
            return elementType is not null;
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
        {
            elementType = type.GetGenericArguments()[0];
            return true;
        }

        foreach (var i in type.GetInterfaces().Where(static i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>)))
        {
            elementType = i.GetGenericArguments()[0];
            return true;
        }

        elementType = null;
        return false;
    }
}

internal sealed class Db2ForeignKeyArrayJoinRow<TLeftKey, TRightKey>
    where TLeftKey : struct
    where TRightKey : struct
{
    public TLeftKey LeftId { get; set; }
    public TRightKey RightId { get; set; }
}
