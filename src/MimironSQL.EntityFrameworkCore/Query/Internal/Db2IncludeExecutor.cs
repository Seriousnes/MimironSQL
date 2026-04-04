using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;

using MimironSQL.EntityFrameworkCore.Infrastructure;
using MimironSQL.EntityFrameworkCore.Model;
using MimironSQL.EntityFrameworkCore.Query.Internal.Visitor;
using MimironSQL.EntityFrameworkCore.Schema;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

internal sealed class Db2IncludeExecutor
{
    internal void ExecuteIncludes<TResult>(QueryContext queryContext, IncludePlan[] includePlans, IReadOnlyList<TResult> results)
    {
        var dbContext = queryContext.Context;

        var store = dbContext.GetService<IMimironDb2Store>();
        var modelBinding = dbContext.GetService<IDb2ModelBinding>().GetBinding();
        var fkGroupingCache = dbContext.GetService<Db2FkGroupingCache>();
        var entityFactory = new DefaultDb2EntityFactory();

        var knownEntities = new List<object>(capacity: results.Count);
        for (var i = 0; i < results.Count; i++)
        {
            var r = results[i];
            if (r is null)
            {
                continue;
            }

            knownEntities.Add((object)r);
        }

        // Cache per-include compiled accessors.
        var getterCache = new Dictionary<(Type ClrType, string Name), Func<object, object?>>(capacity: 16);
        var setterCache = new Dictionary<(Type ClrType, string Name), Action<object, object?>>(capacity: 16);
        var listFactoryCache = new Dictionary<Type, Func<System.Collections.IList>>(capacity: 8);

        // Per-query include cache keyed by dependent table + FK field index.
        // Must not depend on a specific principal-id set; entries incrementally materialize dependents by row ID.
        var oneToManyCache = new Dictionary<(string DependentTable, int ForeignKeyFieldIndex), OneToManyCacheEntry>(capacity: 8);

        var traceIncludes = string.Equals(Environment.GetEnvironmentVariable("MIMIRON_TRACE_INCLUDES"), "1", StringComparison.Ordinal);
        if (traceIncludes)
        {
            Console.WriteLine($"[MimironDb2] Includes: extracted {includePlans.Length} plan(s); root results={results.Count}; knownEntities={knownEntities.Count}");
            for (var i = 0; i < includePlans.Length; i++)
            {
                var p = includePlans[i];
                var nav = p.Navigation;
                var kind = nav is ISkipNavigation ? "skip" : nav is INavigation n && n.IsCollection ? "collection" : "reference";
                Console.WriteLine($"[MimironDb2] IncludePlan[{i}]: source={p.SourceClrType.FullName}; nav={nav.DeclaringEntityType.DisplayName()}.{nav.Name} -> {nav.TargetEntityType.DisplayName()} ({kind})");
            }
        }

        // IncludeExpression nodes in the shaper can be discovered in an order where ThenInclude plans
        // appear before their parent Include plan. Since our batched include executor relies on
        // previously materialized source entities (tracked in knownEntities), execute plans in passes
        // until no further progress is made.
        var executed = new bool[includePlans.Length];
        var remaining = includePlans.Length;
        var madeProgress = true;

        while (remaining > 0 && madeProgress)
        {
            madeProgress = false;

            for (var planIndex = 0; planIndex < includePlans.Length; planIndex++)
            {
                if (executed[planIndex])
                {
                    continue;
                }

                var plan = includePlans[planIndex];

                var sources = knownEntities.Where(e => plan.SourceClrType.IsInstanceOfType(e)).ToArray();
                if (sources.Length == 0)
                {
                    continue;
                }

                var knownBefore = knownEntities.Count;
                if (traceIncludes)
                {
                    Console.WriteLine($"[MimironDb2] Executing IncludePlan[{planIndex}] on {sources.Length} source(s); knownEntities={knownBefore}");
                }

                if (plan.Navigation is ISkipNavigation skipNavigation)
                {
                    ExecuteSkipNavigationInclude(dbContext, store, modelBinding, entityFactory, sources, skipNavigation, getterCache, setterCache, listFactoryCache, knownEntities);
                }
                else
                {
                    if (plan.Navigation is not INavigation navigation)
                    {
                        throw new NotSupportedException($"MimironDb2 Include navigation type '{plan.Navigation.GetType().FullName}' is not supported.");
                    }

                    if (navigation.IsCollection)
                    {
                        ExecuteCollectionNavigationInclude(dbContext, store, modelBinding, entityFactory, fkGroupingCache, sources, navigation, getterCache, setterCache, listFactoryCache, oneToManyCache, knownEntities);
                    }
                    else
                    {
                        ExecuteReferenceNavigationInclude(dbContext, store, modelBinding, entityFactory, sources, navigation, getterCache, setterCache, knownEntities);
                    }
                }

                if (traceIncludes)
                {
                    Console.WriteLine($"[MimironDb2] Executed IncludePlan[{planIndex}]; knownEntities delta={knownEntities.Count - knownBefore}");
                }

                executed[planIndex] = true;
                remaining--;
                madeProgress = true;
            }
        }
    }

    private static void ExecuteSkipNavigationInclude(
        DbContext dbContext,
        IMimironDb2Store store,
        Db2ModelBinding modelBinding,
        IDb2EntityFactory entityFactory,
        IReadOnlyList<object> sources,
        ISkipNavigation navigation,
        Dictionary<(Type ClrType, string Name), Func<object, object?>> getterCache,
        Dictionary<(Type ClrType, string Name), Action<object, object?>> setterCache,
        Dictionary<Type, Func<System.Collections.IList>> listFactoryCache,
        List<object> knownEntities)
    {
        var traceIncludes = string.Equals(Environment.GetEnvironmentVariable("MIMIRON_TRACE_INCLUDES"), "1", StringComparison.Ordinal);

        var joinEntityType = navigation.JoinEntityType;
        var fkArrayPropertyName = joinEntityType
            .FindAnnotation(Db2ForeignKeyArrayModelRewriter.VirtualForeignKeyArrayPropertyAnnotation)
            ?.Value as string;

        if (string.IsNullOrWhiteSpace(fkArrayPropertyName))
        {
            throw new NotSupportedException($"MimironDb2 could not resolve FK array property for skip navigation '{navigation.Name}'.");
        }

        var sourceClrType = navigation.DeclaringEntityType.ClrType;
        var targetClrType = navigation.TargetEntityType.ClrType;

        var pk = navigation.TargetEntityType.FindPrimaryKey();
        if (pk is null || pk.Properties.Count != 1)
        {
            throw new NotSupportedException($"MimironDb2 skip navigation '{navigation.Name}' target must have a single-column primary key.");
        }

        var targetKeyType = pk.Properties[0].ClrType;
        if (Nullable.GetUnderlyingType(targetKeyType) is { } unwrapped)
        {
            targetKeyType = unwrapped;
        }

        if (targetKeyType != typeof(int))
        {
            throw new NotSupportedException($"MimironDb2 skip navigation '{navigation.Name}' currently supports int keys only (saw '{targetKeyType.FullName}').");
        }

        var fkGetter = GetOrCompileGetter(getterCache, sourceClrType, fkArrayPropertyName);
        var navSetter = GetOrCompileSetter(setterCache, sourceClrType, navigation.Name);
        var listFactory = GetOrCompileListFactory(listFactoryCache, targetClrType);

        var allIds = new HashSet<int>();
        var perSourceIds = new List<int[]>(capacity: sources.Count);

        for (var i = 0; i < sources.Count; i++)
        {
            var source = sources[i];
            var idsObj = fkGetter(source);
            if (idsObj is null)
            {
                perSourceIds.Add([]);
                continue;
            }

            if (idsObj is not System.Collections.IEnumerable idsEnumerable)
            {
                throw new NotSupportedException($"MimironDb2 expected FK array '{fkArrayPropertyName}' to be IEnumerable, but got '{idsObj.GetType().FullName}'.");
            }

            var tmp = new List<int>();
            foreach (var idObj in idsEnumerable)
            {
                if (idObj is null)
                {
                    continue;
                }

                var id = Convert.ToInt32(idObj);
                if (id == 0)
                {
                    continue;
                }

                tmp.Add(id);
                allIds.Add(id);
            }

            perSourceIds.Add(tmp.ToArray());
        }

        if (allIds.Count == 0)
        {
            if (traceIncludes)
            {
                Console.WriteLine($"[MimironDb2] Skip include '{navigation.DeclaringEntityType.DisplayName()}.{navigation.Name}': no FK IDs (fkArrayProperty='{fkArrayPropertyName}')");
            }

            // Still mark empty collections as loaded.
            for (var i = 0; i < sources.Count; i++)
            {
                var empty = listFactory();
                navSetter(sources[i], empty);
                dbContext.Entry(sources[i]).Collection(navigation.Name).IsLoaded = true;
            }

            return;
        }

        var targetTableName = navigation.TargetEntityType.GetTableName() ?? targetClrType.Name;
        var loaded = MaterializeByIdsUntyped(store, modelBinding, entityFactory, targetClrType, targetTableName, [.. allIds], takeCount: null);

        if (traceIncludes)
        {
            var sample = allIds.Take(10).ToArray();
            var min = allIds.Count == 0 ? 0 : allIds.Min();
            var max = allIds.Count == 0 ? 0 : allIds.Max();
            Console.WriteLine($"[MimironDb2] Skip include '{navigation.DeclaringEntityType.DisplayName()}.{navigation.Name}': sources={sources.Count}, ids={allIds.Count}, loaded={loaded.Count}, targetTable={targetTableName}, min={min}, max={max}, sample=[{string.Join(",", sample)}]");
        }

        var byId = new Dictionary<int, object>(capacity: loaded.Count);
        var idGetter = GetOrCompileGetter(getterCache, targetClrType, pk.Properties[0].Name);

        Dictionary<int, object>? trackedById = null;
        if (dbContext.ChangeTracker.QueryTrackingBehavior != QueryTrackingBehavior.NoTracking)
        {
            trackedById = new Dictionary<int, object>();
            foreach (var entry in dbContext.ChangeTracker.Entries())
            {
                var trackedEntity = entry.Entity;
                if (trackedEntity is null || trackedEntity.GetType() != targetClrType)
                {
                    continue;
                }

                var trackedIdObj = idGetter(trackedEntity);
                if (trackedIdObj is null)
                {
                    continue;
                }

                trackedById[Convert.ToInt32(trackedIdObj)] = trackedEntity;
            }
        }

        for (var i = 0; i < loaded.Count; i++)
        {
            var entity = loaded[i];
            var idObj = idGetter(entity);
            if (idObj is null)
            {
                continue;
            }

            var id = Convert.ToInt32(idObj);

            if (byId.ContainsKey(id))
            {
                continue;
            }

            if (trackedById is not null && trackedById.TryGetValue(id, out var tracked))
            {
                byId[id] = tracked;
                if (!knownEntities.Contains(tracked))
                {
                    knownEntities.Add(tracked);
                }

                continue;
            }

            TrackIfNeeded(dbContext, entity);
            knownEntities.Add(entity);
            byId[id] = entity;
        }

        for (var i = 0; i < sources.Count; i++)
        {
            var list = listFactory();
            var ids = perSourceIds[i];
            for (var j = 0; j < ids.Length; j++)
            {
                if (byId.TryGetValue(ids[j], out var entity))
                {
                    list.Add(entity);
                }
            }

            navSetter(sources[i], list);
            dbContext.Entry(sources[i]).Collection(navigation.Name).IsLoaded = true;
        }
    }

    private static void ExecuteReferenceNavigationInclude(
        DbContext dbContext,
        IMimironDb2Store store,
        Db2ModelBinding modelBinding,
        IDb2EntityFactory entityFactory,
        IReadOnlyList<object> sources,
        INavigation navigation,
        Dictionary<(Type ClrType, string Name), Func<object, object?>> getterCache,
        Dictionary<(Type ClrType, string Name), Action<object, object?>> setterCache,
        List<object> knownEntities)
    {
        var sourceClrType = navigation.DeclaringEntityType.ClrType;
        var targetClrType = navigation.TargetEntityType.ClrType;

        var sourcePk = navigation.DeclaringEntityType.FindPrimaryKey();
        var targetPk = navigation.TargetEntityType.FindPrimaryKey();
        if (sourcePk is null || sourcePk.Properties.Count != 1 || targetPk is null || targetPk.Properties.Count != 1)
        {
            throw new NotSupportedException($"MimironDb2 reference include '{navigation.Name}' requires single-column PKs.");
        }

        var targetKeyType = targetPk.Properties[0].ClrType;
        if (Nullable.GetUnderlyingType(targetKeyType) is { } unwrapped)
        {
            targetKeyType = unwrapped;
        }

        if (targetKeyType != typeof(int))
        {
            throw new NotSupportedException($"MimironDb2 reference include '{navigation.Name}' currently supports int keys only (saw '{targetKeyType.FullName}').");
        }

        // Shared-PK fast path: principal -> dependent reference can be resolved by principal key.
        // For dependent -> principal references, read FK from dependent.
        var ids = new HashSet<int>();
        Func<object, object?> keyGetter;

        if (!navigation.IsOnDependent)
        {
            // Principal to dependent.
            keyGetter = GetOrCompileGetter(getterCache, sourceClrType, sourcePk.Properties[0].Name);
        }
        else
        {
            // Dependent to principal.
            var fkProperty = navigation.ForeignKey.Properties.Single();
            keyGetter = GetOrCompileGetter(getterCache, sourceClrType, fkProperty.Name);
        }

        for (var i = 0; i < sources.Count; i++)
        {
            var keyObj = keyGetter(sources[i]);
            if (keyObj is null)
            {
                continue;
            }

            var id = Convert.ToInt32(keyObj);
            if (id != 0)
            {
                ids.Add(id);
            }
        }

        if (ids.Count == 0)
        {
            for (var i = 0; i < sources.Count; i++)
            {
                dbContext.Entry(sources[i]).Reference(navigation.Name).IsLoaded = true;
            }

            return;
        }

        var targetTableName = navigation.TargetEntityType.GetTableName() ?? targetClrType.Name;
        var loaded = MaterializeByIdsUntyped(store, modelBinding, entityFactory, targetClrType, targetTableName, [.. ids], takeCount: null);
        var byId = new Dictionary<int, object>(capacity: loaded.Count);
        var targetIdGetter = GetOrCompileGetter(getterCache, targetClrType, targetPk.Properties[0].Name);

        Dictionary<int, object>? trackedById = null;
        if (dbContext.ChangeTracker.QueryTrackingBehavior != QueryTrackingBehavior.NoTracking)
        {
            trackedById = new Dictionary<int, object>();
            foreach (var entry in dbContext.ChangeTracker.Entries())
            {
                var entity = entry.Entity;
                if (entity is null || entity.GetType() != targetClrType)
                {
                    continue;
                }

                var idObj = targetIdGetter(entity);
                if (idObj is null)
                {
                    continue;
                }

                trackedById[Convert.ToInt32(idObj)] = entity;
            }
        }

        for (var i = 0; i < loaded.Count; i++)
        {
            var entity = loaded[i];
            var idObj = targetIdGetter(entity);
            if (idObj is null)
            {
                continue;
            }

            var id = Convert.ToInt32(idObj);

            if (trackedById is not null && trackedById.TryGetValue(id, out var tracked))
            {
                byId[id] = tracked;
                continue;
            }

            TrackIfNeeded(dbContext, entity);
            knownEntities.Add(entity);
            byId[id] = entity;
        }

        var navSetter = GetOrCompileSetter(setterCache, sourceClrType, navigation.Name);

        for (var i = 0; i < sources.Count; i++)
        {
            var source = sources[i];
            var keyObj = keyGetter(source);
            if (keyObj is null)
            {
                navSetter(source, null);
                dbContext.Entry(source).Reference(navigation.Name).IsLoaded = true;
                continue;
            }

            var id = Convert.ToInt32(keyObj);
            byId.TryGetValue(id, out var target);
            navSetter(source, target);

            dbContext.Entry(source).Reference(navigation.Name).IsLoaded = true;
        }
    }

    private static void ExecuteCollectionNavigationInclude(
        DbContext dbContext,
        IMimironDb2Store store,
        Db2ModelBinding modelBinding,
        IDb2EntityFactory entityFactory,
        Db2FkGroupingCache fkGroupingCache,
        IReadOnlyList<object> sources,
        INavigation navigation,
        Dictionary<(Type ClrType, string Name), Func<object, object?>> getterCache,
        Dictionary<(Type ClrType, string Name), Action<object, object?>> setterCache,
        Dictionary<Type, Func<System.Collections.IList>> listFactoryCache,
        Dictionary<(string DependentTable, int ForeignKeyFieldIndex), OneToManyCacheEntry> oneToManyCache,
        List<object> knownEntities)
    {
        var traceIncludes = string.Equals(Environment.GetEnvironmentVariable("MIMIRON_TRACE_INCLUDES"), "1", StringComparison.Ordinal);

        var previousAutoDetect = dbContext.ChangeTracker.AutoDetectChangesEnabled;
        dbContext.ChangeTracker.AutoDetectChangesEnabled = false;
        try
        {
            var sourceClrType = navigation.DeclaringEntityType.ClrType;
            var targetClrType = navigation.TargetEntityType.ClrType;

            var sourcePk = navigation.DeclaringEntityType.FindPrimaryKey();
            if (sourcePk is null || sourcePk.Properties.Count != 1)
            {
                throw new NotSupportedException($"MimironDb2 collection include '{navigation.Name}' requires a single-column principal PK.");
            }

            var principalIdGetter = GetOrCompileGetter(getterCache, sourceClrType, sourcePk.Properties[0].Name);
            var principalIds = new HashSet<int>();
            for (var i = 0; i < sources.Count; i++)
            {
                var idObj = principalIdGetter(sources[i]);
                if (idObj is null)
                {
                    continue;
                }

                var id = Convert.ToInt32(idObj);
                if (id != 0)
                {
                    principalIds.Add(id);
                }
            }

            if (principalIds.Count == 0)
            {
                var navSetterNoIds = GetOrCompileSetter(setterCache, sourceClrType, navigation.Name);
                var listFactoryNoIds = GetOrCompileListFactory(listFactoryCache, targetClrType);

                for (var i = 0; i < sources.Count; i++)
                {
                    navSetterNoIds(sources[i], listFactoryNoIds());
                    dbContext.Entry(sources[i]).Collection(navigation.Name).IsLoaded = true;
                }

                return;
            }

            var dependentTableName = navigation.TargetEntityType.GetTableName() ?? targetClrType.Name;
            var dependentSchema = store.GetSchema(dependentTableName);

            // Resolve FK field index.
            var fkProperty = navigation.ForeignKey.Properties.Single();
            var storeObject = StoreObjectIdentifier.Table(dependentTableName, schema: null);
            var fkColumnName = fkProperty.GetColumnName(storeObject) ?? fkProperty.GetColumnName() ?? fkProperty.Name;
            if (!dependentSchema.TryGetFieldCaseInsensitive(fkColumnName, out var fkField))
            {
                throw new NotSupportedException($"MimironDb2 could not resolve FK column '{fkColumnName}' for include '{navigation.Name}'.");
            }

            if (traceIncludes)
            {
                Console.WriteLine($"[MimironDb2] Collection include '{navigation.DeclaringEntityType.DisplayName()}.{navigation.Name}': sources={sources.Count}, principalIds={principalIds.Count}, dependentTable={dependentTableName}, fkColumn={fkColumnName}, fkFieldIndex={fkField.ColumnStartIndex}");
            }

            var cacheKey = (DependentTable: dependentTableName, ForeignKeyFieldIndex: fkField.ColumnStartIndex);
            if (!oneToManyCache.TryGetValue(cacheKey, out var entry))
            {
                entry = new OneToManyCacheEntry(dependentTableName, dependentSchema.LayoutHash, fkField.ColumnStartIndex);
                oneToManyCache.Add(cacheKey, entry);
            }

            var lookup = entry.GetLookup(store, dependentSchema, modelBinding, entityFactory, fkGroupingCache, principalIds, targetClrType, knownEntities, dbContext, getterCache, setterCache);

            if (traceIncludes)
            {
                Console.WriteLine($"[MimironDb2] Collection include '{navigation.DeclaringEntityType.DisplayName()}.{navigation.Name}': lookup keys={lookup.Count}");
            }

            var navSetter = GetOrCompileSetter(setterCache, sourceClrType, navigation.Name);
            var listFactory = GetOrCompileListFactory(listFactoryCache, targetClrType);

            for (var i = 0; i < sources.Count; i++)
            {
                var source = sources[i];
                var idObj = principalIdGetter(source);
                var id = idObj is null ? 0 : Convert.ToInt32(idObj);

                var list = listFactory();
                if (id != 0 && lookup.TryGetValue(id, out var dependents))
                {
                    for (var j = 0; j < dependents.Count; j++)
                    {
                        list.Add(dependents[j]);
                    }
                }

                navSetter(source, list);
                dbContext.Entry(source).Collection(navigation.Name).IsLoaded = true;
            }
        }
        finally
        {
            dbContext.ChangeTracker.AutoDetectChangesEnabled = previousAutoDetect;
        }
    }

    private sealed class OneToManyCacheEntry(string dependentTableName, string layoutHash, int foreignKeyFieldIndex)
    {
        private readonly string _dependentTableName = dependentTableName;
        private readonly Db2FkGroupingCache.Key _cacheKey = new(dependentTableName, layoutHash, foreignKeyFieldIndex);
        private readonly Dictionary<int, object> _entitiesByRowId = new();
        private readonly Dictionary<int, List<object>> _lookupByPrincipalId = new();

        public Dictionary<int, List<object>> GetLookup(
            IMimironDb2Store store,
            Db2TableSchema dependentSchema,
            Db2ModelBinding modelBinding,
            IDb2EntityFactory entityFactory,
            Db2FkGroupingCache fkGroupingCache,
            HashSet<int> principalIds,
            Type dependentClrType,
            List<object> knownEntities,
            DbContext dbContext,
            Dictionary<(Type ClrType, string Name), Func<object, object?>> getterCache,
            Dictionary<(Type ClrType, string Name), Action<object, object?>> setterCache)
        {
            if (principalIds.Count == 0)
            {
                return [];
            }

            var grouping = fkGroupingCache.GetOrBuild(_cacheKey, () => BuildFkGrouping(store, _dependentTableName, _cacheKey.ForeignKeyFieldIndex));

            var missingRowIds = new HashSet<int>();
            foreach (var principalId in principalIds)
            {
                if (_lookupByPrincipalId.ContainsKey(principalId))
                {
                    continue;
                }

                if (!grouping.TryGetValue(principalId, out var rowIds))
                {
                    continue;
                }

                for (var i = 0; i < rowIds.Length; i++)
                {
                    var rowId = rowIds[i];
                    if (!_entitiesByRowId.ContainsKey(rowId))
                    {
                        missingRowIds.Add(rowId);
                    }
                }
            }

            if (missingRowIds.Count > 0)
            {
                EnsureDependentsMaterialized(_entitiesByRowId, missingRowIds, store, _dependentTableName, dependentSchema, modelBinding, entityFactory, dependentClrType, dbContext, knownEntities, getterCache, setterCache);
            }

            foreach (var principalId in principalIds)
            {
                if (_lookupByPrincipalId.ContainsKey(principalId))
                {
                    continue;
                }

                if (!grouping.TryGetValue(principalId, out var dependentRowIds))
                {
                    continue;
                }

                if (dependentRowIds.Length == 0)
                {
                    continue;
                }

                var list = new List<object>(capacity: dependentRowIds.Length);
                for (var i = 0; i < dependentRowIds.Length; i++)
                {
                    if (_entitiesByRowId.TryGetValue(dependentRowIds[i], out var entity))
                    {
                        list.Add(entity);
                    }
                }

                if (list.Count > 0)
                {
                    _lookupByPrincipalId[principalId] = list;
                }
            }

            var result = new Dictionary<int, List<object>>(capacity: principalIds.Count);
            foreach (var principalId in principalIds)
            {
                if (_lookupByPrincipalId.TryGetValue(principalId, out var dependents))
                {
                    result[principalId] = dependents;
                }
            }

            return result;
        }
    }

    private static void EnsureDependentsMaterialized(
        Dictionary<int, object> entitiesByRowId,
        HashSet<int> rowIdsToLoad,
        IMimironDb2Store store,
        string dependentTableName,
        Db2TableSchema dependentSchema,
        Db2ModelBinding modelBinding,
        IDb2EntityFactory entityFactory,
        Type dependentClrType,
        DbContext dbContext,
        List<object> knownEntities,
        Dictionary<(Type ClrType, string Name), Func<object, object?>> getterCache,
        Dictionary<(Type ClrType, string Name), Action<object, object?>> setterCache)
    {
        if (rowIdsToLoad.Count == 0)
        {
            return;
        }

        var (file, _) = store.OpenTableWithSchema<RowHandle>(dependentTableName);

        var db2EntityType = modelBinding.GetEntityType(dependentClrType).WithSchema(dependentTableName, dependentSchema);

        var materializerType = typeof(Db2EntityMaterializer<>).MakeGenericType(dependentClrType);
        var materializer = Activator.CreateInstance(materializerType, modelBinding, db2EntityType, entityFactory)
            ?? throw new InvalidOperationException($"Failed to create Db2EntityMaterializer for '{dependentClrType.FullName}'.");

        var materializeMethod = materializerType.GetMethod(nameof(Db2EntityMaterializer<object>.Materialize))
            ?? throw new InvalidOperationException("Db2EntityMaterializer.Materialize method was not found.");

        Func<object, object?>? dependentIdGetter = null;
        Action<object, object?>? dependentIdSetter = null;
        try
        {
            dependentIdGetter = GetOrCompileGetter(getterCache, dependentClrType, "Id");
            dependentIdSetter = GetOrCompileSetter(setterCache, dependentClrType, "Id");
        }
        catch (NotSupportedException)
        {
        }

        Dictionary<int, object>? trackedById = null;
        if (dbContext.ChangeTracker.QueryTrackingBehavior != QueryTrackingBehavior.NoTracking)
        {
            trackedById = new Dictionary<int, object>();
            foreach (var entry in dbContext.ChangeTracker.Entries())
            {
                var trackedEntity = entry.Entity;
                if (trackedEntity is null || trackedEntity.GetType() != dependentClrType)
                {
                    continue;
                }

                if (dependentIdGetter is null)
                {
                    continue;
                }

                var idObj = dependentIdGetter(trackedEntity);
                if (idObj is null)
                {
                    continue;
                }

                trackedById[Convert.ToInt32(idObj)] = trackedEntity;
            }
        }

        var handles = new List<RowHandle>(capacity: rowIdsToLoad.Count);
        foreach (var rowId in rowIdsToLoad)
        {
            if (entitiesByRowId.ContainsKey(rowId))
            {
                continue;
            }

            if (file.TryGetRowById(rowId, out var handle))
            {
                handles.Add(handle);
            }
        }

        if (handles.Count == 0)
        {
            return;
        }

        handles.Sort(static (a, b) =>
        {
            var section = a.SectionIndex.CompareTo(b.SectionIndex);
            if (section != 0)
            {
                return section;
            }

            return a.RowIndexInSection.CompareTo(b.RowIndexInSection);
        });

        for (var i = 0; i < handles.Count; i++)
        {
            var handle = handles[i];
            var rowId = handle.RowId;

            if (entitiesByRowId.ContainsKey(rowId))
            {
                continue;
            }

            object entity;
            if (trackedById is not null && trackedById.TryGetValue(rowId, out var tracked))
            {
                entity = tracked;
                if (!knownEntities.Contains(tracked))
                {
                    knownEntities.Add(tracked);
                }
            }
            else
            {
                entity = (object)materializeMethod.Invoke(materializer, [file, handle])!;

                if (dependentIdGetter is not null && dependentIdSetter is not null)
                {
                    var idObj = dependentIdGetter(entity);
                    var id = idObj is null ? 0 : Convert.ToInt32(idObj);
                    if (id == 0)
                    {
                        dependentIdSetter(entity, rowId);
                    }
                }

                TrackIfNeeded(dbContext, entity);
                knownEntities.Add(entity);

                trackedById?.TryAdd(rowId, entity);
            }

            entitiesByRowId[rowId] = entity;
        }
    }

    private static IReadOnlyDictionary<int, int[]> BuildFkGrouping(
        IMimironDb2Store store,
        string dependentTableName,
        int foreignKeyFieldIndex)
    {
        var (file, _) = store.OpenTableWithSchema<RowHandle>(dependentTableName);

        var temp = new Dictionary<int, List<int>>();
        foreach (var handle in file.EnumerateRowHandles())
        {
            var fk = file.ReadField<int>(handle, foreignKeyFieldIndex);
            if (fk == 0)
            {
                continue;
            }

            if (!temp.TryGetValue(fk, out var list))
            {
                list = [];
                temp.Add(fk, list);
            }

            list.Add(handle.RowId);
        }

        if (temp.Count == 0)
        {
            return new Dictionary<int, int[]>();
        }

        var result = new Dictionary<int, int[]>(capacity: temp.Count);
        foreach (var (fk, list) in temp)
        {
            result.Add(fk, [.. list]);
        }

        return result;
    }

    private static IReadOnlyList<object> MaterializeByIdsUntyped(
        IMimironDb2Store store,
        Db2ModelBinding modelBinding,
        IDb2EntityFactory entityFactory,
        Type entityClrType,
        string tableName,
        IReadOnlyList<int> ids,
        int? takeCount)
    {
        var method = typeof(IMimironDb2Store)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Single(m => m.Name == nameof(IMimironDb2Store.MaterializeByIds) && m.IsGenericMethodDefinition);

        var generic = method.MakeGenericMethod(entityClrType);
        var result = generic.Invoke(store, [tableName, ids, takeCount, modelBinding, entityFactory]);
        if (result is not System.Collections.IEnumerable e)
        {
            return [];
        }

        var list = new List<object>();
        foreach (var item in e)
        {
            if (item is not null)
            {
                list.Add(item);
            }
        }

        return list;
    }

    private static Func<object, object?> GetOrCompileGetter(Dictionary<(Type ClrType, string Name), Func<object, object?>> cache, Type clrType, string name)
    {
        if (cache.TryGetValue((clrType, name), out var existing))
        {
            return existing;
        }

        var property = clrType.GetProperty(name, BindingFlags.Instance | BindingFlags.Public)
            ?? throw new NotSupportedException($"Property '{clrType.FullName}.{name}' was not found.");

        var obj = Expression.Parameter(typeof(object), "obj");
        var cast = Expression.Convert(obj, clrType);
        var access = Expression.Property(cast, property);
        var box = Expression.Convert(access, typeof(object));
        var lambda = Expression.Lambda<Func<object, object?>>(box, obj).Compile();

        cache[(clrType, name)] = lambda;
        return lambda;
    }

    private static Action<object, object?> GetOrCompileSetter(Dictionary<(Type ClrType, string Name), Action<object, object?>> cache, Type clrType, string name)
    {
        if (cache.TryGetValue((clrType, name), out var existing))
        {
            return existing;
        }

        var property = clrType.GetProperty(name, BindingFlags.Instance | BindingFlags.Public)
            ?? throw new NotSupportedException($"Property '{clrType.FullName}.{name}' was not found.");

        if (property.SetMethod is null)
        {
            throw new NotSupportedException($"Property '{clrType.FullName}.{name}' must be writable for include fixup.");
        }

        var obj = Expression.Parameter(typeof(object), "obj");
        var value = Expression.Parameter(typeof(object), "value");
        var castObj = Expression.Convert(obj, clrType);
        var castValue = Expression.Convert(value, property.PropertyType);
        var assign = Expression.Assign(Expression.Property(castObj, property), castValue);
        var lambda = Expression.Lambda<Action<object, object?>>(assign, obj, value).Compile();

        cache[(clrType, name)] = lambda;
        return lambda;
    }

    private static Func<System.Collections.IList> GetOrCompileListFactory(Dictionary<Type, Func<System.Collections.IList>> cache, Type elementType)
    {
        if (cache.TryGetValue(elementType, out var existing))
        {
            return existing;
        }

        var listType = typeof(List<>).MakeGenericType(elementType);
        var ctor = listType.GetConstructor(Type.EmptyTypes)
            ?? throw new NotSupportedException($"List<{elementType.Name}> constructor was not found.");

        var newExpr = Expression.New(ctor);
        var cast = Expression.Convert(newExpr, typeof(System.Collections.IList));
        var lambda = Expression.Lambda<Func<System.Collections.IList>>(cast).Compile();

        cache[elementType] = lambda;
        return lambda;
    }

    private static void TrackIfNeeded(DbContext dbContext, object entity)
    {
        var entry = dbContext.Entry(entity);
        if (entry.State != EntityState.Detached)
        {
            return;
        }

        // These entities come from a read-only store. EF's key conventions frequently mark integer keys as
        // ValueGeneratedOnAdd, and EF treats default values (e.g. 0) as temporary, which triggers generation.
        // Clear the temporary flags so we can track query results even when the key happens to be the default.
        if (entry.Metadata.FindPrimaryKey() is { } pk)
        {
            for (var i = 0; i < pk.Properties.Count; i++)
            {
                entry.Property(pk.Properties[i].Name).IsTemporary = false;
            }
        }

        try
        {
            entry.State = EntityState.Unchanged;
        }
        catch (NotSupportedException)
        {
            // During bootstrap, some models still use key generation conventions which reject
            // entities with default key values. Since DB2 is read-only, we can still proceed
            // with include fixup without tracking these entities.
        }
    }
}
