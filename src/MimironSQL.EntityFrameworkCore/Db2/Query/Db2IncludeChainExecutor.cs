using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.Extensions;
using MimironSQL.Formats;

namespace MimironSQL.Db2.Query;

internal static class Db2IncludeChainExecutor
{
    private static readonly ConcurrentDictionary<(Type EntityType, MemberInfo Member), Delegate> GetterCache = new();
    private static readonly ConcurrentDictionary<(Type EntityType, MemberInfo Member), Delegate> SetterCache = new();
    private static readonly ConcurrentDictionary<(Type EntityType, MemberInfo Member), Delegate> IntGetterCache = new();
    private static readonly ConcurrentDictionary<(Type EntityType, MemberInfo Member), Delegate> IntEnumerableGetterCache = new();

    public static IEnumerable<TEntity> Apply<TEntity, TRow>(
        IEnumerable<TEntity> source,
        Db2Model model,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        IReadOnlyList<MemberInfo> members,
        IDb2EntityFactory entityFactory)
        where TRow : struct, IRowHandle
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(tableResolver);
        ArgumentNullException.ThrowIfNull(members);
        ArgumentNullException.ThrowIfNull(entityFactory);

        if (members.Count == 0)
            return source;

        var roots = source as List<TEntity> ?? source.ToList();
        if (roots.Count == 0)
            return roots;

        var currentEntities = new List<object>(roots.Count);
        for (var i = 0; i < roots.Count; i++)
            currentEntities.Add(roots[i]!);

        var currentType = typeof(TEntity);

        for (var i = 0; i < members.Count; i++)
        {
            if (currentEntities.Count == 0)
                break;

            var navMember = members[i];

            if (!IsWritable(navMember))
                throw new NotSupportedException($"Navigation member '{navMember.Name}' must be writable.");

            if (model.TryGetReferenceNavigation(currentType, navMember, out var referenceNav))
            {
                ApplyReferenceInclude<TRow>(currentEntities, currentType, navMember, referenceNav, model, tableResolver, entityFactory);
                currentEntities = CollectNextEntities(currentEntities, currentType, navMember);
                currentType = referenceNav.TargetClrType;
                continue;
            }

            if (model.TryGetCollectionNavigation(currentType, navMember, out var collectionNav))
            {
                ApplyCollectionInclude<TRow>(currentEntities, currentType, navMember, collectionNav, model, tableResolver, entityFactory);
                currentEntities = CollectNextEntities(currentEntities, currentType, navMember);
                currentType = collectionNav.TargetClrType;
                continue;
            }

            var memberType = navMember.GetMemberType();
            if (memberType.IsValueType || memberType == typeof(string))
            {
                throw new NotSupportedException(
                    "Include only supports navigation member access chains (e.g., x => x.Parent or x => x.Parent.Child).");
            }

            throw new NotSupportedException(
                $"Include navigation '{currentType.FullName}.{navMember.Name}' is not configured. Configure the navigation in OnModelCreating, or ensure schema FK conventions can apply.");
        }

        return roots;
    }

    private static void ApplyReferenceInclude<TRow>(
        List<object> entities,
        Type entityType,
        MemberInfo navMember,
        Db2ReferenceNavigation navigation,
        Db2Model model,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        IDb2EntityFactory entityFactory)
        where TRow : struct, IRowHandle
    {
        if (!IsReadable(navigation.SourceKeyMember))
            throw new NotSupportedException($"Navigation key member '{navigation.SourceKeyMember.Name}' must be readable.");

        if (navigation.Kind != Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey &&
            navigation.Kind != Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne)
        {
            throw new NotSupportedException(
                $"Include navigation '{entityType.FullName}.{navMember.Name}' has unsupported kind '{navigation.Kind}'.");
        }

        if (!navigation.TargetClrType.IsClass)
            throw new NotSupportedException($"Reference navigation '{entityType.FullName}.{navMember.Name}' target type must be a reference type.");

        var keyGetter = GetOrCreateIntGetter(entityType, navigation.SourceKeyMember);
        var setter = GetOrCreateSetter(entityType, navMember);

        var targetEntityType = model.GetEntityType(navigation.TargetClrType);
        var (relatedFile, _) = tableResolver(targetEntityType.TableName);
        var materializer = new Db2EntityMaterializerFactory<TRow>(targetEntityType, entityFactory);

        var entitiesWithKeys = new List<(object Entity, int Key)>(entities.Count);
        HashSet<int> keys = [];

        for (var i = 0; i < entities.Count; i++)
        {
            var entity = entities[i];
            var key = keyGetter(entity);
            entitiesWithKeys.Add((entity, key));
            if (key != 0)
                keys.Add(key);
        }

        Dictionary<int, object> relatedByKey = new(capacity: Math.Min(keys.Count, relatedFile.RecordsCount));
        if (keys.Count != 0)
        {
            foreach (var row in relatedFile.EnumerateRows())
            {
                var rowId = Db2RowHandleAccess.AsHandle(row).RowId;
                if (!keys.Contains(rowId))
                    continue;

                relatedByKey[rowId] = materializer.Materialize(relatedFile, Db2RowHandleAccess.AsHandle(row));
            }
        }

        for (var i = 0; i < entitiesWithKeys.Count; i++)
        {
            var (entity, key) = entitiesWithKeys[i];

            relatedByKey.TryGetValue(key, out var related);
            setter(entity, related);
        }
    }

    private static void ApplyCollectionInclude<TRow>(
        List<object> entities,
        Type entityType,
        MemberInfo navMember,
        Db2CollectionNavigation navigation,
        Db2Model model,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        IDb2EntityFactory entityFactory)
        where TRow : struct, IRowHandle
    {
        if (!navigation.TargetClrType.IsClass)
            throw new NotSupportedException($"Collection navigation '{entityType.FullName}.{navMember.Name}' target type must be a reference type.");

        switch (navigation.Kind)
        {
            case Db2CollectionNavigationKind.ForeignKeyArrayToPrimaryKey:
                ApplyForeignKeyArrayToPrimaryKeyInclude(entities, entityType, navMember, navigation, model, tableResolver, entityFactory);
                return;
            case Db2CollectionNavigationKind.DependentForeignKeyToPrimaryKey:
                ApplyDependentForeignKeyToPrimaryKeyInclude<TRow>(entities, entityType, navMember, navigation, model, tableResolver, entityFactory);
                return;
            default:
                throw new NotSupportedException(
                    $"Include navigation '{entityType.FullName}.{navMember.Name}' has unsupported kind '{navigation.Kind}'.");
        }
    }

    private static void ApplyForeignKeyArrayToPrimaryKeyInclude<TRow>(
        List<object> entities,
        Type entityType,
        MemberInfo navMember,
        Db2CollectionNavigation navigation,
        Db2Model model,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        IDb2EntityFactory entityFactory)
        where TRow : struct, IRowHandle
    {
        if (navigation.SourceKeyCollectionMember is null || navigation.SourceKeyFieldSchema is null)
            throw new NotSupportedException($"Collection navigation '{entityType.FullName}.{navMember.Name}' must specify a source key collection member.");

        if (!IsReadable(navigation.SourceKeyCollectionMember))
            throw new NotSupportedException($"Navigation key member '{navigation.SourceKeyCollectionMember.Name}' must be readable.");

        var keyListGetter = GetOrCreateIntEnumerableGetter(entityType, navigation.SourceKeyCollectionMember);
        var setter = GetOrCreateSetter(entityType, navMember);
        var navMemberType = navMember.GetMemberType();

        if (navMemberType.IsArray)
        {
            throw new NotSupportedException(
                $"Collection navigation '{entityType.FullName}.{navMember.Name}' must be declared as ICollection<{navigation.TargetClrType.Name}> (array-typed collection navigations are not supported). ");
        }

        var targetEntityType = model.GetEntityType(navigation.TargetClrType);
        var (relatedFile, _) = tableResolver(targetEntityType.TableName);
        var materializer = new Db2EntityMaterializerFactory<TRow>(targetEntityType, entityFactory);

        var entitiesWithIds = new List<(object Entity, int[] Ids)>(entities.Count);
        HashSet<int> keys = [];

        for (var i = 0; i < entities.Count; i++)
        {
            var entity = entities[i];
            var idsEnumerable = keyListGetter(entity);
            var ids = idsEnumerable as int[] ?? idsEnumerable?.ToArray() ?? [];

            entitiesWithIds.Add((entity, ids));
            for (var j = 0; j < ids.Length; j++)
            {
                var id = ids[j];
                if (id != 0)
                    keys.Add(id);
            }
        }

        Dictionary<int, object> relatedByKey = new(capacity: Math.Min(keys.Count, relatedFile.RecordsCount));
        if (keys.Count != 0)
        {
            foreach (var row in relatedFile.EnumerateRows())
            {
                var rowId = Db2RowHandleAccess.AsHandle(row).RowId;
                if (!keys.Contains(rowId))
                    continue;

                relatedByKey[rowId] = materializer.Materialize(relatedFile, Db2RowHandleAccess.AsHandle(row));
            }
        }

        for (var i = 0; i < entitiesWithIds.Count; i++)
        {
            var (entity, ids) = entitiesWithIds[i];

            var count = 0;
            for (var j = 0; j < ids.Length; j++)
            {
                var id = ids[j];
                if (id == 0)
                    continue;

                if (relatedByKey.ContainsKey(id))
                    count++;
            }

            var collection = CreateCollectionInstance(navMemberType, navigation.TargetClrType, capacity: count);
            var buffer = ((IArrayBackedReadOnlyCollection)collection).Buffer;

            var index = 0;
            for (var j = 0; j < ids.Length; j++)
            {
                var id = ids[j];
                if (id == 0)
                    continue;

                if (!relatedByKey.TryGetValue(id, out var related))
                    continue;

                buffer.SetValue(related, index);
                index++;
            }

            setter(entity, collection);
        }
    }

    private static void ApplyDependentForeignKeyToPrimaryKeyInclude<TRow>(
        List<object> entities,
        Type entityType,
        MemberInfo navMember,
        Db2CollectionNavigation navigation,
        Db2Model model,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        IDb2EntityFactory entityFactory)
        where TRow : struct, IRowHandle
    {
        if (navigation.PrincipalKeyMember is null)
            throw new NotSupportedException($"Collection navigation '{entityType.FullName}.{navMember.Name}' must specify a principal key member.");

        if (navigation.DependentForeignKeyFieldSchema is null)
            throw new NotSupportedException($"Collection navigation '{entityType.FullName}.{navMember.Name}' must specify a dependent foreign key field schema.");

        if (!IsReadable(navigation.PrincipalKeyMember))
            throw new NotSupportedException($"Principal key member '{navigation.PrincipalKeyMember.Name}' must be readable.");

        var setter = GetOrCreateSetter(entityType, navMember);
        var principalKeyGetter = GetOrCreateIntGetter(entityType, navigation.PrincipalKeyMember);
        var navMemberType = navMember.GetMemberType();

        if (navMemberType.IsArray)
        {
            throw new NotSupportedException(
                $"Collection navigation '{entityType.FullName}.{navMember.Name}' must be declared as ICollection<{navigation.TargetClrType.Name}> (array-typed collection navigations are not supported). ");
        }

        var entitiesWithKeys = new List<(object Entity, int Key)>(entities.Count);
        HashSet<int> keys = [];

        for (var i = 0; i < entities.Count; i++)
        {
            var entity = entities[i];
            var key = principalKeyGetter(entity);
            entitiesWithKeys.Add((entity, key));
            if (key != 0)
                keys.Add(key);
        }

        var targetEntityType = model.GetEntityType(navigation.TargetClrType);
        var (relatedFile, _) = tableResolver(targetEntityType.TableName);
        var materializer = new Db2EntityMaterializerFactory<TRow>(targetEntityType, entityFactory);

        Dictionary<int, List<object>> dependentsByKey = [];
        if (keys.Count != 0)
        {
            var fkFieldIndex = navigation.DependentForeignKeyFieldSchema.Value.ColumnStartIndex;
            foreach (var row in relatedFile.EnumerateRows())
            {
                var fk = Db2RowHandleAccess.ReadField<TRow, int>(relatedFile, row, fkFieldIndex);
                if (fk == 0 || !keys.Contains(fk))
                    continue;

                var dependent = materializer.Materialize(relatedFile, Db2RowHandleAccess.AsHandle(row));
                if (!dependentsByKey.TryGetValue(fk, out var list))
                {
                    list = [];
                    dependentsByKey.Add(fk, list);
                }

                list.Add(dependent);
            }
        }

        for (var i = 0; i < entitiesWithKeys.Count; i++)
        {
            var (entity, key) = entitiesWithKeys[i];

            if (!dependentsByKey.TryGetValue(key, out var dependents) || dependents.Count == 0)
            {
                setter(entity, CreateCollectionInstance(navMemberType, navigation.TargetClrType, capacity: 0));
                continue;
            }

            var collection = CreateCollectionInstance(navMemberType, navigation.TargetClrType, capacity: dependents.Count);
            var buffer = ((IArrayBackedReadOnlyCollection)collection).Buffer;
            for (var j = 0; j < dependents.Count; j++)
                buffer.SetValue(dependents[j], j);

            setter(entity, collection);
        }
    }

    private interface IArrayBackedReadOnlyCollection
    {
        Array Buffer { get; }
    }

    private sealed class ArrayBackedReadOnlyCollection<T>(T[] buffer)
        : ReadOnlyCollection<T>(buffer), IArrayBackedReadOnlyCollection
    {
        public Array Buffer { get; } = buffer;
    }

    private static object CreateCollectionInstance(Type navigationMemberType, Type elementType, int capacity)
    {
        var expectedType = typeof(ICollection<>).MakeGenericType(elementType);
        if (navigationMemberType != expectedType)
        {
            throw new NotSupportedException(
                $"Collection navigation type '{navigationMemberType.FullName}' is not supported. " +
                $"Collection navigation properties must be declared as '{expectedType.FullName}'.");
        }

        var buffer = Array.CreateInstance(elementType, capacity);
        var wrapperType = typeof(ArrayBackedReadOnlyCollection<>).MakeGenericType(elementType);
        return Activator.CreateInstance(wrapperType, buffer)!;
    }

    private static List<object> CollectNextEntities(List<object> entities, Type entityType, MemberInfo navMember)
    {
        var getter = GetOrCreateGetter(entityType, navMember);

        var seen = new HashSet<object>(ReferenceEqualityComparer.Instance);
        var next = new List<object>();

        for (var i = 0; i < entities.Count; i++)
        {
            var entity = entities[i];
            var value = getter(entity);

            switch (value)
            {
                case null:
                    continue;
                case string:
                    continue;
                case IEnumerable enumerable:
                    foreach (var item in enumerable)
                    {
                        if (item is null)
                            continue;

                        if (seen.Add(item))
                            next.Add(item);
                    }

                    continue;
                default:
                    if (seen.Add(value))
                        next.Add(value);

                    continue;
            }
        }

        return next;
    }

    private static Func<object, object?> GetOrCreateGetter(Type entityType, MemberInfo member)
        => (Func<object, object?>)GetterCache.GetOrAdd((entityType, member), static key =>
        {
            var instance = Expression.Parameter(typeof(object), "instance");
            var typedInstance = Expression.Convert(instance, key.EntityType);

            Expression access = key.Member switch
            {
                PropertyInfo p => Expression.Property(typedInstance, p),
                FieldInfo f => Expression.Field(typedInstance, f),
                _ => throw new InvalidOperationException($"Unexpected member type: {key.Member.GetType().FullName}"),
            };

            var body = Expression.Convert(access, typeof(object));
            return Expression.Lambda<Func<object, object?>>(body, instance).Compile();
        });

    private static Action<object, object?> GetOrCreateSetter(Type entityType, MemberInfo member)
        => (Action<object, object?>)SetterCache.GetOrAdd((entityType, member), static key =>
        {
            var instance = Expression.Parameter(typeof(object), "instance");
            var value = Expression.Parameter(typeof(object), "value");

            var typedInstance = Expression.Convert(instance, key.EntityType);

            var memberType = key.Member.GetMemberType();

            Expression access = key.Member switch
            {
                PropertyInfo p => Expression.Property(typedInstance, p),
                FieldInfo f => Expression.Field(typedInstance, f),
                _ => throw new InvalidOperationException($"Unexpected member type: {key.Member.GetType().FullName}"),
            };

            var assign = Expression.Assign(access, Expression.Convert(value, memberType));
            return Expression.Lambda<Action<object, object?>>(assign, instance, value).Compile();
        });

    private static Func<object, int> GetOrCreateIntGetter(Type entityType, MemberInfo member)
        => (Func<object, int>)IntGetterCache.GetOrAdd((entityType, member), static key =>
        {
            var instance = Expression.Parameter(typeof(object), "instance");
            var typedInstance = Expression.Convert(instance, key.EntityType);

            Expression access = key.Member switch
            {
                PropertyInfo p => Expression.Property(typedInstance, p),
                FieldInfo f => Expression.Field(typedInstance, f),
                _ => throw new InvalidOperationException($"Unexpected member type: {key.Member.GetType().FullName}"),
            };

            var memberType = key.Member.GetMemberType();
            access = ConvertToInt32NoBox(access, memberType);
            return Expression.Lambda<Func<object, int>>(access, instance).Compile();
        });

    private static Func<object, IEnumerable<int>?> GetOrCreateIntEnumerableGetter(Type entityType, MemberInfo member)
        => (Func<object, IEnumerable<int>?>)IntEnumerableGetterCache.GetOrAdd((entityType, member), static key =>
        {
            var instance = Expression.Parameter(typeof(object), "instance");
            var typedInstance = Expression.Convert(instance, key.EntityType);

            Expression access = key.Member switch
            {
                PropertyInfo p => Expression.Property(typedInstance, p),
                FieldInfo f => Expression.Field(typedInstance, f),
                _ => throw new InvalidOperationException($"Unexpected member type: {key.Member.GetType().FullName}"),
            };

            var memberType = key.Member.GetMemberType();

            if (memberType == typeof(int[]))
                return Expression.Lambda<Func<object, IEnumerable<int>?>>(Expression.Convert(access, typeof(IEnumerable<int>)), instance).Compile();

            if (typeof(IEnumerable<int>).IsAssignableFrom(memberType))
                return Expression.Lambda<Func<object, IEnumerable<int>?>>(Expression.Convert(access, typeof(IEnumerable<int>)), instance).Compile();

            if (!TryGetIEnumerableElementType(memberType, out var elementType) || elementType is null)
                throw new NotSupportedException($"Expected an integer key enumerable member but found '{memberType.FullName}'.");

            var enumerableType = typeof(IEnumerable<>).MakeGenericType(elementType);
            var castEnumerable = Expression.Convert(access, enumerableType);

            var item = Expression.Parameter(elementType, "x");
            var itemToInt = ConvertToInt32NoBox(item, elementType);
            var selector = Expression.Lambda(itemToInt, item);

            var selectCall = Expression.Call(
                typeof(Enumerable),
                nameof(Enumerable.Select),
                [elementType, typeof(int)],
                castEnumerable,
                selector);

            return Expression.Lambda<Func<object, IEnumerable<int>?>>(selectCall, instance).Compile();
        });

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

    private static Expression ConvertToInt32NoBox(Expression value, Type valueType)
    {
        if (Nullable.GetUnderlyingType(valueType) is { } nullableUnderlying)
        {
            var getValueOrDefault = valueType.GetMethod(nameof(Nullable<>.GetValueOrDefault), Type.EmptyTypes)!;
            value = Expression.Call(value, getValueOrDefault);
            valueType = nullableUnderlying;
        }

        if (valueType.IsEnum)
        {
            var underlying = Enum.GetUnderlyingType(valueType);
            value = Expression.Convert(value, underlying);
            valueType = underlying;
        }

        return valueType == typeof(int)
            ? value
            : Expression.Convert(value, typeof(int));
    }

    private static bool IsReadable(MemberInfo member)
        => member switch
        {
            PropertyInfo p => p.GetMethod is not null,
            FieldInfo => true,
            _ => false,
        };

    private static bool IsWritable(MemberInfo member)
        => member switch
        {
            PropertyInfo p => p.SetMethod is not null,
            FieldInfo f => !f.IsInitOnly,
            _ => false,
        };

    private sealed class Db2EntityMaterializerFactory<TRow>
        where TRow : struct, IRowHandle
    {
        private readonly object _materializer;
        private readonly Func<IDb2File<TRow>, RowHandle, object> _materialize;

        public Db2EntityMaterializerFactory(Db2EntityType entityType, IDb2EntityFactory entityFactory)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ArgumentNullException.ThrowIfNull(entityFactory);

            var targetClrType = entityType.ClrType;
            var materializerType = typeof(Db2EntityMaterializer<,>).MakeGenericType(targetClrType, typeof(TRow));

            _materializer = Activator.CreateInstance(materializerType, entityType, entityFactory)
                ?? throw new InvalidOperationException($"Unable to create materializer for {targetClrType.FullName}.");

            var fileParam = Expression.Parameter(typeof(IDb2File<TRow>), "file");
            var handleParam = Expression.Parameter(typeof(RowHandle), "handle");

            var call = Expression.Call(
                Expression.Convert(Expression.Constant(_materializer), materializerType),
                materializerType.GetMethod(nameof(Db2EntityMaterializer<object, TRow>.Materialize))!,
                fileParam,
                handleParam);

            var body = Expression.Convert(call, typeof(object));
            _materialize = Expression.Lambda<Func<IDb2File<TRow>, RowHandle, object>>(body, fileParam, handleParam).Compile();
        }

        public object Materialize(IDb2File<TRow> file, RowHandle handle) => _materialize(file, handle);
    }
}
