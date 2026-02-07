using System.Collections.Concurrent;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.EntityFrameworkCore.Storage;

namespace MimironSQL.EntityFrameworkCore.Query;

internal interface IMimironDb2Db2ModelProvider
{
    Db2Model GetDb2Model();
}

internal sealed class MimironDb2Db2ModelProvider(
    ICurrentDbContext currentDbContext,
    IMimironDb2Store store) : IMimironDb2Db2ModelProvider
{
    private static readonly ConcurrentDictionary<Type, MethodInfo> EntityGenericMethodCache = new();

    private readonly DbContext _context = currentDbContext?.Context ?? throw new ArgumentNullException(nameof(currentDbContext));
    private readonly IMimironDb2Store _store = store ?? throw new ArgumentNullException(nameof(store));

    private Db2Model? _model;

    public Db2Model GetDb2Model() => _model ??= BuildDb2Model();

    private Db2Model BuildDb2Model()
    {
        var efModel = _context.Model;

        var builder = new Db2ModelBuilder();

        foreach (var entityType in efModel.GetEntityTypes().OrderBy(static e => e.ClrType.FullName, StringComparer.Ordinal))
        {
            if (entityType.ClrType is null)
                continue;

            ConfigureEntity(builder, entityType);
        }

        foreach (var entityType in efModel.GetEntityTypes().OrderBy(static e => e.ClrType.FullName, StringComparer.Ordinal))
        {
            if (entityType.ClrType is null)
                continue;

            ConfigureNavigations(builder, entityType);
        }

        return builder.Build(tableName => _store.GetSchema(tableName));
    }

    private static void ConfigureEntity(Db2ModelBuilder builder, IEntityType entityType)
    {
        var clrType = entityType.ClrType;

        var db2EntityBuilder = InvokeEntity(builder, clrType);

        var tableName = entityType.GetTableName() ?? clrType.Name;

        var tableAttr = clrType.GetCustomAttribute<TableAttribute>(inherit: true);
        if (tableAttr is null)
        {
            db2EntityBuilder
                .GetType()
                .GetMethod(nameof(Db2EntityTypeBuilder<object>.ToTable), BindingFlags.Public | BindingFlags.Instance)!
                .Invoke(db2EntityBuilder, [tableName]);
        }

        foreach (var property in entityType.GetProperties())
        {
            if (property.PropertyInfo is null)
                continue;

            if (property.IsShadowProperty())
                continue;

            if (property.Name.Equals("Id", StringComparison.OrdinalIgnoreCase))
                continue;

            var columnName = property.GetColumnName() ?? property.Name;

            var propInfo = property.PropertyInfo;

            if (propInfo.GetCustomAttribute<ColumnAttribute>(inherit: true) is not null)
                continue;

            if (string.Equals(columnName, propInfo.Name, StringComparison.Ordinal))
                continue;

            var propBuilder = InvokeProperty(db2EntityBuilder, clrType, propInfo);
            propBuilder
                .GetType()
                .GetMethod(nameof(Db2PropertyBuilder<object>.HasColumnName), BindingFlags.Public | BindingFlags.Instance)!
                .Invoke(propBuilder, [columnName]);
        }
    }

    private static void ConfigureNavigations(Db2ModelBuilder builder, IEntityType entityType)
    {
        foreach (var navigation in entityType.GetNavigations())
        {
            if (navigation.PropertyInfo is null)
                continue;

            var fk = navigation.ForeignKey;

            // v1: only handle simple 1-column FK -> 1-column PK
            if (fk.Properties.Count != 1 || fk.PrincipalKey.Properties.Count != 1)
                continue;

            var navProperty = navigation.PropertyInfo;

            if (navigation.IsCollection)
            {
                // principal = entityType, dependent = target
                var dependentClr = navigation.TargetEntityType.ClrType;

                var dependentFkProp = fk.Properties[0].PropertyInfo;
                if (dependentFkProp is null)
                    continue;

                var principalKeyProp = fk.PrincipalKey.Properties[0].PropertyInfo;
                if (principalKeyProp is null)
                    continue;

                var hasManyBuilder = InvokeHasMany(builder, entityType.ClrType, dependentClr, navProperty);

                var withFk = hasManyBuilder.GetType().GetMethod(nameof(Db2CollectionNavigationBuilder<object, object>.WithForeignKey))!;
                var withFkGeneric = withFk.MakeGenericMethod(dependentFkProp.PropertyType);
                withFkGeneric.Invoke(hasManyBuilder, [BuildPropertyLambda(dependentClr, dependentFkProp)]);

                var hasPrincipalKey = hasManyBuilder.GetType().GetMethod(nameof(Db2CollectionNavigationBuilder<object, object>.HasPrincipalKey))!;
                var hasPrincipalKeyGeneric = hasPrincipalKey.MakeGenericMethod(principalKeyProp.PropertyType);
                hasPrincipalKeyGeneric.Invoke(hasManyBuilder, [BuildPropertyLambda(entityType.ClrType, principalKeyProp)]);

                continue;
            }

            // Reference navigation
            // dependent = entityType, principal = target
            var sourceClr = entityType.ClrType;
            var targetClr = navigation.TargetEntityType.ClrType;

            var sourceFkProp = fk.Properties[0].PropertyInfo;
            if (sourceFkProp is null)
                continue;

            var principalKey = fk.PrincipalKey.Properties[0].PropertyInfo;
            if (principalKey is null)
                continue;

            var hasOneBuilder = InvokeHasOne(builder, sourceClr, targetClr, navProperty);

            var withForeignKey = hasOneBuilder.GetType().GetMethod(nameof(Db2ReferenceNavigationBuilder<object, object>.WithForeignKey))!;
            var withForeignKeyGeneric = withForeignKey.MakeGenericMethod(sourceFkProp.PropertyType);
            withForeignKeyGeneric.Invoke(hasOneBuilder, [BuildPropertyLambda(sourceClr, sourceFkProp)]);

            var hasPrincipalKeyMethod = hasOneBuilder.GetType().GetMethod(nameof(Db2ReferenceNavigationBuilder<object, object>.HasPrincipalKey))!;
            var hasPrincipalKeyGenericMethod = hasPrincipalKeyMethod.MakeGenericMethod(principalKey.PropertyType);
            hasPrincipalKeyGenericMethod.Invoke(hasOneBuilder, [BuildPropertyLambda(targetClr, principalKey)]);
        }
    }

    private static object InvokeEntity(Db2ModelBuilder builder, Type clrType)
    {
        var m = EntityGenericMethodCache.GetOrAdd(clrType, static clrType =>
            typeof(Db2ModelBuilder)
                .GetMethod(nameof(Db2ModelBuilder.Entity), BindingFlags.Public | BindingFlags.Instance)!
                .MakeGenericMethod(clrType));

        return m.Invoke(builder, [])!;
    }

    private static object InvokeProperty(object db2EntityBuilder, Type entityClrType, PropertyInfo property)
    {
        var propertyMethod = db2EntityBuilder.GetType().GetMethod(nameof(Db2EntityTypeBuilder<object>.Property))!;
        var generic = propertyMethod.MakeGenericMethod(property.PropertyType);
        return generic.Invoke(db2EntityBuilder, [BuildPropertyLambda(entityClrType, property)])!;
    }

    private static object InvokeHasOne(Db2ModelBuilder builder, Type sourceClr, Type targetClr, PropertyInfo navigation)
    {
        var db2EntityBuilder = InvokeEntity(builder, sourceClr);
        var method = db2EntityBuilder.GetType().GetMethod(nameof(Db2EntityTypeBuilder<object>.HasOne))!;
        var generic = method.MakeGenericMethod(targetClr);
        return generic.Invoke(db2EntityBuilder, [BuildPropertyLambda(sourceClr, navigation)])!;
    }

    private static object InvokeHasMany(Db2ModelBuilder builder, Type sourceClr, Type targetClr, PropertyInfo navigation)
    {
        var db2EntityBuilder = InvokeEntity(builder, sourceClr);
        var method = db2EntityBuilder.GetType().GetMethod(nameof(Db2EntityTypeBuilder<object>.HasMany))!;
        var generic = method.MakeGenericMethod(targetClr);
        return generic.Invoke(db2EntityBuilder, [BuildEnumerableNavigationLambda(sourceClr, targetClr, navigation)])!;
    }

    private static LambdaExpression BuildPropertyLambda(Type parameterType, PropertyInfo property)
    {
        var param = Expression.Parameter(parameterType, "x");
        var body = Expression.Property(param, property);
        var delegateType = typeof(Func<,>).MakeGenericType(parameterType, property.PropertyType);
        return Expression.Lambda(delegateType, body, param);
    }

    private static LambdaExpression BuildEnumerableNavigationLambda(Type sourceClr, Type targetClr, PropertyInfo navigation)
    {
        var param = Expression.Parameter(sourceClr, "x");
        Expression body = Expression.Property(param, navigation);

        var enumerableTargetType = typeof(IEnumerable<>).MakeGenericType(targetClr);
        if (body.Type != enumerableTargetType)
            body = Expression.Convert(body, enumerableTargetType);

        var delegateType = typeof(Func<,>).MakeGenericType(sourceClr, enumerableTargetType);
        return Expression.Lambda(delegateType, body, param);
    }
}
