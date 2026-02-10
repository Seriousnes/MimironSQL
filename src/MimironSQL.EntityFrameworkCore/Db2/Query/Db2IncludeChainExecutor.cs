using System.Collections;
using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.EntityFrameworkCore.Extensions;
using MimironSQL.Formats;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

internal static class Db2IncludeChainExecutor
{
    private static readonly ConcurrentDictionary<(Type EntityType, MemberInfo Member, Type ValueType), Delegate> TypedGetterCache = new();
    private static readonly ConcurrentDictionary<(Type EntityType, MemberInfo Member, Type ValueType), Delegate> TypedSetterCache = new();
    private static readonly ConcurrentDictionary<(Type EntityType, MemberInfo Member), Delegate> TypedIntGetterCache = new();
    private static readonly ConcurrentDictionary<(Type EntityType, MemberInfo Member), Delegate> TypedIntEnumerableGetterCache = new();

    public static IEnumerable<TEntity> Apply<TEntity, TRow>(
        IEnumerable<TEntity> source,
        Db2Model model,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        IReadOnlyList<MemberInfo> members,
        IDb2EntityFactory entityFactory)
        where TEntity : class
        where TRow : struct, IRowHandle
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(tableResolver);
        ArgumentNullException.ThrowIfNull(members);
        ArgumentNullException.ThrowIfNull(entityFactory);

        if (members.Count == 0)
            return source;

        var roots = source as List<TEntity> ?? [.. source];
        if (roots.Count == 0)
            return roots;

        IIncludeEntityList current = new IncludeEntityList<TEntity>(roots);
        var currentType = typeof(TEntity);

        for (var i = 0; i < members.Count; i++)
        {
            if (current.Count == 0)
                break;

            var navMember = members[i];

            if (!IsWritable(navMember))
                throw new NotSupportedException($"Navigation member '{navMember.Name}' must be writable.");

            if (model.TryGetReferenceNavigation(currentType, navMember, out var referenceNav))
            {
                current = ReferenceIncludeDispatcher<TRow>.Invoke(
                    currentType,
                    referenceNav.TargetClrType,
                    current,
                    navMember,
                    referenceNav,
                    model,
                    tableResolver,
                    entityFactory);
                currentType = referenceNav.TargetClrType;
                continue;
            }

            if (model.TryGetCollectionNavigation(currentType, navMember, out var collectionNav))
            {
                current = CollectionIncludeDispatcher<TRow>.Invoke(
                    currentType,
                    collectionNav.TargetClrType,
                    current,
                    navMember,
                    collectionNav,
                    model,
                    tableResolver,
                    entityFactory);
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

    private interface IIncludeEntityList
    {
        Type EntityType { get; }
        int Count { get; }
    }

    private sealed class IncludeEntityList<TEntity>(List<TEntity> items) : IIncludeEntityList
        where TEntity : class
    {
        public List<TEntity> Items { get; } = items;
        public Type EntityType => typeof(TEntity);
        public int Count => Items.Count;
    }

    private static class ReferenceIncludeDispatcher<TRow>
        where TRow : struct, IRowHandle
    {
        private static readonly ConcurrentDictionary<(Type EntityType, Type TargetType), Func<
            IIncludeEntityList,
            MemberInfo,
            Db2ReferenceNavigation,
            Db2Model,
            Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)>,
            IDb2EntityFactory,
            IIncludeEntityList>> Cache = new();

        public static IIncludeEntityList Invoke(
            Type entityType,
            Type targetType,
            IIncludeEntityList current,
            MemberInfo navMember,
            Db2ReferenceNavigation navigation,
            Db2Model model,
            Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
            IDb2EntityFactory entityFactory)
        {
            var del = Cache.GetOrAdd((entityType, targetType), Create);
            return del(current, navMember, navigation, model, tableResolver, entityFactory);
        }

        private static Func<
            IIncludeEntityList,
            MemberInfo,
            Db2ReferenceNavigation,
            Db2Model,
            Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)>,
            IDb2EntityFactory,
            IIncludeEntityList> Create((Type EntityType, Type TargetType) key)
        {
            var method = typeof(Db2IncludeChainExecutor)
                .GetMethod(nameof(ApplyReferenceIncludeAndCollectNext), BindingFlags.Static | BindingFlags.NonPublic)!
                .MakeGenericMethod(key.EntityType, key.TargetType, typeof(TRow));

            return (Func<
                IIncludeEntityList,
                MemberInfo,
                Db2ReferenceNavigation,
                Db2Model,
                Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)>,
                IDb2EntityFactory,
                IIncludeEntityList>)method.CreateDelegate(typeof(Func<
                    IIncludeEntityList,
                    MemberInfo,
                    Db2ReferenceNavigation,
                    Db2Model,
                    Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)>,
                    IDb2EntityFactory,
                    IIncludeEntityList>));
        }
    }

    private static class CollectionIncludeDispatcher<TRow>
        where TRow : struct, IRowHandle
    {
        private static readonly ConcurrentDictionary<(Type EntityType, Type TargetType), Func<
            IIncludeEntityList,
            MemberInfo,
            Db2CollectionNavigation,
            Db2Model,
            Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)>,
            IDb2EntityFactory,
            IIncludeEntityList>> ForeignKeyArrayCache = new();

        private static readonly ConcurrentDictionary<(Type EntityType, Type TargetType), Func<
            IIncludeEntityList,
            MemberInfo,
            Db2CollectionNavigation,
            Db2Model,
            Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)>,
            IDb2EntityFactory,
            IIncludeEntityList>> DependentForeignKeyCache = new();

        public static IIncludeEntityList Invoke(
            Type entityType,
            Type targetType,
            IIncludeEntityList current,
            MemberInfo navMember,
            Db2CollectionNavigation navigation,
            Db2Model model,
            Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
            IDb2EntityFactory entityFactory)
        {
            return navigation.Kind switch
            {
                Db2CollectionNavigationKind.ForeignKeyArrayToPrimaryKey =>
                    ForeignKeyArrayCache.GetOrAdd((entityType, targetType), CreateForeignKeyArray)(current, navMember, navigation, model, tableResolver, entityFactory),

                Db2CollectionNavigationKind.DependentForeignKeyToPrimaryKey =>
                    DependentForeignKeyCache.GetOrAdd((entityType, targetType), CreateDependentForeignKey)(current, navMember, navigation, model, tableResolver, entityFactory),

                _ => throw new NotSupportedException($"Include collection navigation has unsupported kind '{navigation.Kind}'."),
            };
        }

        private static Func<
            IIncludeEntityList,
            MemberInfo,
            Db2CollectionNavigation,
            Db2Model,
            Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)>,
            IDb2EntityFactory,
            IIncludeEntityList> CreateForeignKeyArray((Type EntityType, Type TargetType) key)
        {
            var method = typeof(Db2IncludeChainExecutor)
                .GetMethod(nameof(ApplyForeignKeyArrayIncludeAndCollectNext), BindingFlags.Static | BindingFlags.NonPublic)!
                .MakeGenericMethod(key.EntityType, key.TargetType, typeof(TRow));

            return (Func<
                IIncludeEntityList,
                MemberInfo,
                Db2CollectionNavigation,
                Db2Model,
                Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)>,
                IDb2EntityFactory,
                IIncludeEntityList>)method.CreateDelegate(typeof(Func<
                    IIncludeEntityList,
                    MemberInfo,
                    Db2CollectionNavigation,
                    Db2Model,
                    Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)>,
                    IDb2EntityFactory,
                    IIncludeEntityList>));
        }

        private static Func<
            IIncludeEntityList,
            MemberInfo,
            Db2CollectionNavigation,
            Db2Model,
            Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)>,
            IDb2EntityFactory,
            IIncludeEntityList> CreateDependentForeignKey((Type EntityType, Type TargetType) key)
        {
            var method = typeof(Db2IncludeChainExecutor)
                .GetMethod(nameof(ApplyDependentForeignKeyIncludeAndCollectNext), BindingFlags.Static | BindingFlags.NonPublic)!
                .MakeGenericMethod(key.EntityType, key.TargetType, typeof(TRow));

            return (Func<
                IIncludeEntityList,
                MemberInfo,
                Db2CollectionNavigation,
                Db2Model,
                Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)>,
                IDb2EntityFactory,
                IIncludeEntityList>)method.CreateDelegate(typeof(Func<
                    IIncludeEntityList,
                    MemberInfo,
                    Db2CollectionNavigation,
                    Db2Model,
                    Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)>,
                    IDb2EntityFactory,
                    IIncludeEntityList>));
        }
    }

    private static IIncludeEntityList ApplyReferenceIncludeAndCollectNext<TEntity, TTarget, TRow>(
        IIncludeEntityList current,
        MemberInfo navMember,
        Db2ReferenceNavigation navigation,
        Db2Model model,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        IDb2EntityFactory entityFactory)
        where TEntity : class
        where TTarget : class
        where TRow : struct, IRowHandle
    {
        var entities = ((IncludeEntityList<TEntity>)current).Items;
        ApplyReferenceIncludeTyped<TEntity, TTarget, TRow>(entities, navMember, navigation, model, tableResolver, entityFactory);

        var getter = GetOrCreateTypedGetter<TEntity, TTarget>(typeof(TEntity), navMember);

        var seen = new HashSet<TTarget>(ReferenceEqualityComparer<TTarget>.Instance);
        var next = new List<TTarget>();

        for (var i = 0; i < entities.Count; i++)
        {
            var related = getter(entities[i]);
            if (related is null)
                continue;

            if (seen.Add(related))
                next.Add(related);
        }

        return new IncludeEntityList<TTarget>(next);
    }

    private static IIncludeEntityList ApplyForeignKeyArrayIncludeAndCollectNext<TEntity, TTarget, TRow>(
        IIncludeEntityList current,
        MemberInfo navMember,
        Db2CollectionNavigation navigation,
        Db2Model model,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        IDb2EntityFactory entityFactory)
        where TEntity : class
        where TTarget : class
        where TRow : struct, IRowHandle
    {
        var entities = ((IncludeEntityList<TEntity>)current).Items;
        ApplyForeignKeyArrayToPrimaryKeyIncludeTyped<TEntity, TTarget, TRow>(entities, navMember, navigation, model, tableResolver, entityFactory);

        var getter = GetOrCreateTypedGetter<TEntity, ICollection<TTarget>>(typeof(TEntity), navMember);

        var seen = new HashSet<TTarget>(ReferenceEqualityComparer<TTarget>.Instance);
        var next = new List<TTarget>();

        for (var i = 0; i < entities.Count; i++)
        {
            var collection = getter(entities[i]);
            if (collection is null)
                continue;

            if (collection is ArrayBackedReadOnlyCollection<TTarget> abc)
            {
                var buffer = abc.Buffer;
                for (var j = 0; j < buffer.Length; j++)
                {
                    var item = buffer[j];
                    if (item is null)
                        continue;

                    if (seen.Add(item))
                        next.Add(item);
                }

                continue;
            }

            foreach (var item in collection)
            {
                if (item is null)
                    continue;

                if (seen.Add(item))
                    next.Add(item);
            }
        }

        return new IncludeEntityList<TTarget>(next);
    }

    private static IIncludeEntityList ApplyDependentForeignKeyIncludeAndCollectNext<TEntity, TTarget, TRow>(
        IIncludeEntityList current,
        MemberInfo navMember,
        Db2CollectionNavigation navigation,
        Db2Model model,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        IDb2EntityFactory entityFactory)
        where TEntity : class
        where TTarget : class
        where TRow : struct, IRowHandle
    {
        var entities = ((IncludeEntityList<TEntity>)current).Items;
        ApplyDependentForeignKeyToPrimaryKeyIncludeTyped<TEntity, TTarget, TRow>(entities, navMember, navigation, model, tableResolver, entityFactory);

        var getter = GetOrCreateTypedGetter<TEntity, ICollection<TTarget>>(typeof(TEntity), navMember);

        var seen = new HashSet<TTarget>(ReferenceEqualityComparer<TTarget>.Instance);
        var next = new List<TTarget>();

        for (var i = 0; i < entities.Count; i++)
        {
            var collection = getter(entities[i]);
            if (collection is null)
                continue;

            if (collection is ArrayBackedReadOnlyCollection<TTarget> abc)
            {
                var buffer = abc.Buffer;
                for (var j = 0; j < buffer.Length; j++)
                {
                    var item = buffer[j];
                    if (item is null)
                        continue;

                    if (seen.Add(item))
                        next.Add(item);
                }

                continue;
            }

            foreach (var item in collection)
            {
                if (item is null)
                    continue;

                if (seen.Add(item))
                    next.Add(item);
            }
        }

        return new IncludeEntityList<TTarget>(next);
    }

    private static void ApplyReferenceIncludeTyped<TEntity, TTarget, TRow>(
        List<TEntity> entities,
        MemberInfo navMember,
        Db2ReferenceNavigation navigation,
        Db2Model model,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        IDb2EntityFactory entityFactory)
        where TEntity : class
        where TTarget : class
        where TRow : struct, IRowHandle
    {
        if (!IsReadable(navigation.SourceKeyMember))
            throw new NotSupportedException($"Navigation key member '{navigation.SourceKeyMember.Name}' must be readable.");

        if (navigation.Kind != Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey &&
            navigation.Kind != Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne)
        {
            throw new NotSupportedException(
                $"Include navigation '{typeof(TEntity).FullName}.{navMember.Name}' has unsupported kind '{navigation.Kind}'.");
        }

        var keyGetter = GetOrCreateTypedIntGetter<TEntity>(typeof(TEntity), navigation.SourceKeyMember);
        var setter = GetOrCreateTypedSetter<TEntity, TTarget>(typeof(TEntity), navMember);

        var targetEntityType = model.GetEntityType(navigation.TargetClrType);
        var (relatedFile, _) = tableResolver(targetEntityType.TableName);
        var materializer = new Db2EntityMaterializer<TTarget, TRow>(targetEntityType, entityFactory);

        var entitiesWithKeys = new List<(TEntity Entity, int Key)>(entities.Count);
        HashSet<int> keys = [];

        for (var i = 0; i < entities.Count; i++)
        {
            var entity = entities[i];
            var key = keyGetter(entity);
            entitiesWithKeys.Add((entity, key));
            if (key != 0)
                keys.Add(key);
        }

        Dictionary<int, TTarget> relatedByKey = new(capacity: Math.Min(keys.Count, relatedFile.RecordsCount));
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

    private static void ApplyForeignKeyArrayToPrimaryKeyIncludeTyped<TEntity, TTarget, TRow>(
        List<TEntity> entities,
        MemberInfo navMember,
        Db2CollectionNavigation navigation,
        Db2Model model,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        IDb2EntityFactory entityFactory)
        where TEntity : class
        where TTarget : class
        where TRow : struct, IRowHandle
    {
        if (navigation.SourceKeyCollectionMember is null || navigation.SourceKeyFieldSchema is null)
            throw new NotSupportedException($"Collection navigation '{typeof(TEntity).FullName}.{navMember.Name}' must specify a source key collection member.");

        if (!IsReadable(navigation.SourceKeyCollectionMember))
            throw new NotSupportedException($"Navigation key member '{navigation.SourceKeyCollectionMember.Name}' must be readable.");

        var keyListGetter = GetOrCreateTypedIntEnumerableGetter<TEntity>(typeof(TEntity), navigation.SourceKeyCollectionMember);
        var setter = GetOrCreateTypedSetter<TEntity, ICollection<TTarget>>(typeof(TEntity), navMember);
        var navMemberType = navMember.GetMemberType();

        if (navMemberType.IsArray)
        {
            throw new NotSupportedException(
                $"Collection navigation '{typeof(TEntity).FullName}.{navMember.Name}' must be declared as ICollection<{typeof(TTarget).Name}> (array-typed collection navigations are not supported). ");
        }

        var targetEntityType = model.GetEntityType(navigation.TargetClrType);
        var (relatedFile, _) = tableResolver(targetEntityType.TableName);
        var materializer = new Db2EntityMaterializer<TTarget, TRow>(targetEntityType, entityFactory);

        var entitiesWithIds = new List<(TEntity Entity, int[] Ids)>(entities.Count);
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

        Dictionary<int, TTarget> relatedByKey = new(capacity: Math.Min(keys.Count, relatedFile.RecordsCount));
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

            var collection = CreateCollectionInstance(navMemberType, capacity: count, out TTarget[] buffer);

            var index = 0;
            for (var j = 0; j < ids.Length; j++)
            {
                var id = ids[j];
                if (id == 0)
                    continue;

                if (!relatedByKey.TryGetValue(id, out var related))
                    continue;

                buffer[index] = related;
                index++;
            }

            setter(entity, collection);
        }
    }

    private static void ApplyDependentForeignKeyToPrimaryKeyIncludeTyped<TEntity, TTarget, TRow>(
        List<TEntity> entities,
        MemberInfo navMember,
        Db2CollectionNavigation navigation,
        Db2Model model,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        IDb2EntityFactory entityFactory)
        where TEntity : class
        where TTarget : class
        where TRow : struct, IRowHandle
    {
        if (navigation.PrincipalKeyMember is null)
            throw new NotSupportedException($"Collection navigation '{typeof(TEntity).FullName}.{navMember.Name}' must specify a principal key member.");

        if (navigation.DependentForeignKeyFieldSchema is null)
            throw new NotSupportedException($"Collection navigation '{typeof(TEntity).FullName}.{navMember.Name}' must specify a dependent foreign key field schema.");

        if (!IsReadable(navigation.PrincipalKeyMember))
            throw new NotSupportedException($"Principal key member '{navigation.PrincipalKeyMember.Name}' must be readable.");

        var setter = GetOrCreateTypedSetter<TEntity, ICollection<TTarget>>(typeof(TEntity), navMember);
        var principalKeyGetter = GetOrCreateTypedIntGetter<TEntity>(typeof(TEntity), navigation.PrincipalKeyMember);
        var navMemberType = navMember.GetMemberType();

        if (navMemberType.IsArray)
        {
            throw new NotSupportedException(
                $"Collection navigation '{typeof(TEntity).FullName}.{navMember.Name}' must be declared as ICollection<{typeof(TTarget).Name}> (array-typed collection navigations are not supported). ");
        }

        var entitiesWithKeys = new List<(TEntity Entity, int Key)>(entities.Count);
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
        var materializer = new Db2EntityMaterializer<TTarget, TRow>(targetEntityType, entityFactory);

        Dictionary<int, List<TTarget>> dependentsByKey = [];
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
                setter(entity, CreateCollectionInstance(navMemberType, capacity: 0, out TTarget[] _));
                continue;
            }

            var collection = CreateCollectionInstance(navMemberType, capacity: dependents.Count, out TTarget[] buffer);
            for (var j = 0; j < dependents.Count; j++)
                buffer[j] = dependents[j];

            setter(entity, collection);
        }
    }

    private sealed class ArrayBackedReadOnlyCollection<T>(T[] buffer)
        : ReadOnlyCollection<T>(buffer)
        where T : class
    {
        public T[] Buffer { get; } = buffer;
    }

    private static ICollection<TElement> CreateCollectionInstance<TElement>(Type navigationMemberType, int capacity, out TElement[] buffer)
        where TElement : class
    {
        var expectedType = typeof(ICollection<TElement>);
        if (navigationMemberType != expectedType)
        {
            throw new NotSupportedException(
                $"Collection navigation type '{navigationMemberType.FullName}' is not supported. " +
                $"Collection navigation properties must be declared as '{expectedType.FullName}'.");
        }

        buffer = new TElement[capacity];
        return new ArrayBackedReadOnlyCollection<TElement>(buffer);
    }

    private sealed class ReferenceEqualityComparer<T> : IEqualityComparer<T>
        where T : class
    {
        public static ReferenceEqualityComparer<T> Instance { get; } = new();

        public bool Equals(T? x, T? y) => ReferenceEquals(x, y);

        public int GetHashCode(T obj) => System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(obj);
    }

    private static Func<TEntity, TMember?> GetOrCreateTypedGetter<TEntity, TMember>(Type entityType, MemberInfo member)
        where TEntity : class
        where TMember : class
        => (Func<TEntity, TMember?>)TypedGetterCache.GetOrAdd((entityType, member, typeof(TMember)), static key =>
        {
            var instance = Expression.Parameter(key.EntityType, "instance");

            Expression access = key.Member switch
            {
                PropertyInfo p => Expression.Property(instance, p),
                FieldInfo f => Expression.Field(instance, f),
                _ => throw new InvalidOperationException($"Unexpected member type: {key.Member.GetType().FullName}"),
            };

            if (access.Type != key.ValueType)
                access = Expression.Convert(access, key.ValueType);

            return Expression.Lambda<Func<TEntity, TMember?>>(access, instance).Compile();
        });

    private static Action<TEntity, TMember?> GetOrCreateTypedSetter<TEntity, TMember>(Type entityType, MemberInfo member)
        where TEntity : class
        where TMember : class
        => (Action<TEntity, TMember?>)TypedSetterCache.GetOrAdd((entityType, member, typeof(TMember)), static key =>
        {
            var instance = Expression.Parameter(key.EntityType, "instance");
            var value = Expression.Parameter(key.ValueType, "value");

            Expression access = key.Member switch
            {
                PropertyInfo p => Expression.Property(instance, p),
                FieldInfo f => Expression.Field(instance, f),
                _ => throw new InvalidOperationException($"Unexpected member type: {key.Member.GetType().FullName}"),
            };

            var assign = Expression.Assign(access, value);
            return Expression.Lambda<Action<TEntity, TMember?>>(assign, instance, value).Compile();
        });

    private static Func<TEntity, int> GetOrCreateTypedIntGetter<TEntity>(Type entityType, MemberInfo member)
        where TEntity : class
        => (Func<TEntity, int>)TypedIntGetterCache.GetOrAdd((entityType, member), static key =>
        {
            var instance = Expression.Parameter(key.EntityType, "instance");

            Expression access = key.Member switch
            {
                PropertyInfo p => Expression.Property(instance, p),
                FieldInfo f => Expression.Field(instance, f),
                _ => throw new InvalidOperationException($"Unexpected member type: {key.Member.GetType().FullName}"),
            };

            var memberType = key.Member.GetMemberType();
            access = ConvertToInt32NoBox(access, memberType);
            return Expression.Lambda<Func<TEntity, int>>(access, instance).Compile();
        });

    private static Func<TEntity, IEnumerable<int>?> GetOrCreateTypedIntEnumerableGetter<TEntity>(Type entityType, MemberInfo member)
        where TEntity : class
        => (Func<TEntity, IEnumerable<int>?>)TypedIntEnumerableGetterCache.GetOrAdd((entityType, member), static key =>
        {
            var instance = Expression.Parameter(key.EntityType, "instance");

            Expression access = key.Member switch
            {
                PropertyInfo p => Expression.Property(instance, p),
                FieldInfo f => Expression.Field(instance, f),
                _ => throw new InvalidOperationException($"Unexpected member type: {key.Member.GetType().FullName}"),
            };

            var memberType = key.Member.GetMemberType();

            if (memberType == typeof(int[]))
                return Expression.Lambda<Func<TEntity, IEnumerable<int>?>>(Expression.Convert(access, typeof(IEnumerable<int>)), instance).Compile();

            if (typeof(IEnumerable<int>).IsAssignableFrom(memberType))
                return Expression.Lambda<Func<TEntity, IEnumerable<int>?>>(Expression.Convert(access, typeof(IEnumerable<int>)), instance).Compile();

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

            return Expression.Lambda<Func<TEntity, IEnumerable<int>?>>(selectCall, instance).Compile();
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
}
