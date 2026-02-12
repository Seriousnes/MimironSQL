# Performance Analysis: Can_query_db2context_for_spell()
The SingleOrDefault(x => x.Id == 454009) query takes 774ms. Based on the profiler counters and code trace, here are the key bottleneck areas and improvement suggestions:

## 1. JIT Compilation Overhead (~29% of wall time)
|Metric|Value|
|-|-|
|Methods|JIT'd|+4,460|
|JIT time|+223.5 ms|
|IL Bytes Jitted|+575 KB|

The query path triggers massive first-time JIT compilation:
- Db2EntityMaterializer<TEntity, TRow> compiles Expression.Lambda<Action<...>> delegates per-property in CreateBindings(Db2EntityType) (line 285 in Db2EntityMaterializer.cs). For a SpellEntity with many columns, each property binding compiles a new delegate.
- TryInvokeProxyFactory(object, IEntityType, bool, out object) uses runtime reflection to resolve proxy factories, iterating loaded assemblies with AppDomain.CurrentDomain.GetAssemblies() and calling GetType() on each one.
- ExecuteDelegates.GetOrAdd in MimironDb2QueryExecutor uses MakeGenericMethod + CreateDelegate on first call.

Suggestions:
- Cache Db2EntityMaterializer<TEntity, TRow> instances across queries for the same entity type. Currently, a new materializer is created every time MaterializeByIdsFromOpenFile<TEntity>(IDb2File<RowHandle>, Db2TableSchema, string, IReadOnlyList<int>, int?, Db2ModelBinding, IDb2EntityFactory) is called (line 384). Since the bindings are pure functions of the entity type + schema, cache them.
- Cache the EfLazyLoadingProxyDb2EntityFactory itself (or at least cache the proxy factory resolution). ResolveTypeByFullName(string) does a full scan of AppDomain.CurrentDomain.GetAssemblies() every call.
- Pre-warm the materializer bindings during model creation (`_ = context.Model`) rather than at first query time.

## 2. Redundant Parse(Stream) Calls
The expression tree is parsed 3 times for a key-lookup query:
1. Line 65: `var pipeline = Db2QueryPipeline.Parse(query);`
2. Line 260 (inside TryGetKeyLookupRequest<TEntity>(Expression, string, out KeyLookupRequest)): `var pipeline = Db2QueryPipeline.Parse(query);`
3. Line 79 (inside Warm(Expression, Type)): `var pipeline = Db2QueryPipeline.Parse(query);`

Suggestion: Parse the pipeline once and pass the result through to TryGetKeyLookupRequest<TEntity>(Expression, string, out KeyLookupRequest) and Warm(Expression, Type). This eliminates two redundant expression tree walks.

## 3. GC Pressure and Memory Allocation (~34% time in GC)
|Metric|Value|
|-|-|
|% Time in GC|+34%|
|Gen 1 growth|+5.7 MB|
|Gen 2 growth|+5.4 MB|
|Working Set|+22 MB|

Key allocation sources:
- EnsureIndexesBuilt() (line 684): Allocates a Dictionary<int, (int, int, int)> with RecordsCount. For the Spell table (large table with potentially 400k+ records), this is a large up-front allocation that promotes into Gen 2. Each value tuple is 12 bytes + dictionary overhead.
- Dictionary<int, int> for _copyMap (line 690): Created without initial capacity estimation.
- CASC stream reading: OpenDb2StreamAsync(string, CancellationToken) decodes BLTE blocks into a MemoryStream, which may involve internal buffer resizing.
- String table materialization: EnsureDenseStringTableMaterialized() allocates new byte[Header.StringTableSize] and seeks the stream multiple times.
- Per-binding delegate compilation: Each CreateBinding(PropertyInfo, Type, int) call in the materializer creates a Func<> via Expression.Lambda.Compile().

Suggestions:
- Use FrozenDictionary for _idIndex after building — it's built once and read many times. FrozenDictionary has lower per-lookup overhead and better memory density.
- Pre-size _copyMap with section.CopyTableCount capacity.
- Pool or reuse the dense string table buffer if the same table is queried repeatedly.
- Consider using int[] with binary search instead of Dictionary for the ID index if IDs are mostly contiguous. For WDC5 Spell tables, IDs often have reasonable density and sorted arrays are more cache-friendly.

## 4. I/O: Lazy Record Loading + Per-Field Stream Seeks
When RecordLoadingMode is Lazy (the default), ReadField<T> for a single row that hasn't been materialized calls GetRowBytesFromStream(int, Wdc5Section, int, out int, out int) (line 877), which:
1. Seeks the underlying CASC stream to the row offset
2. Reads the row bytes into _cachedRowBytes

For a SingleOrDefault query that only materializes one row, this is fine — but the materializer calls ReadField<T> once per property on the same row. While the single-row cache at line 905 prevents re-reading the stream for the same row, the initial CASC stream seek/read is still expensive.

However, the key-lookup path at line 83 calls MaterializeByIdsFromOpenFile<TEntity>(IDb2File<RowHandle>, Db2TableSchema, string, IReadOnlyList<int>, int?, Db2ModelBinding, IDb2EntityFactory), which calls TryGetRowById<TId>(TId, out RowHandle) (triggering EnsureIndexesBuilt() → reads all IndexData). For a single-ID lookup, building the entire index just to find one row is wasteful.

Suggestions:
- For single-ID lookups, consider a linear scan of IndexData instead of building the full dictionary first. If IndexData is present and the query only needs one ID, a simple Array.IndexOf or loop is O(n) but avoids the O(n) dictionary allocation + GC pressure.
- Alternatively, eagerly build and cache the index on the Wdc5File so it's amortized across queries on the same file. Currently the file is opened fresh per query (OpenTableWithSchema(string) creates a new Wdc5File each time at line 72 of IMimironDb2Store.cs), so the index is rebuilt every time.

## 5. No File/Index Caching Across Queries
OpenTableWithSchema(string) (line 67) opens a fresh stream and Wdc5File on every call. For the Spell table, this means:
- Parsing the entire WDC5 header + section metadata
- Building the id index from scratch
- Reading the dense string table

QuerySession caches within a single query, but across queries (e.g., the test's constructor vs. the test method), there's no sharing.

Suggestion: Implement a table-level file cache in MimironDb2Store (or a higher-level cache) that keeps parsed Wdc5File instances alive across queries. This is the single highest-impact optimization — it would eliminate the ~774ms cold-start cost on repeat queries to the same table.

## Summary of Recommended Optimizations (by expected impact)
|Priority|Optimization|Expected Impact|
|-|-|-|
|🔴 High|Cache opened Wdc5File instances across queries|Eliminates repeated I/O, parsing, and index building|
|🔴 High|Cache Db2EntityMaterializer bindings per entity type|Eliminates repeated delegate compilation (JIT)|
|🟡 Medium|Parse Db2QueryPipeline once per query execution|Removes 2 redundant expression tree walks|
|🟡 Medium|Use FrozenDictionary for _idIndex|Reduces GC pressure and lookup overhead|
|🟡 Medium|Cache proxy factory resolution in EfLazyLoadingProxyDb2EntityFactory|Removes AppDomain.GetAssemblies() scans|
|🟢 Low|Pre-size _copyMap dictionary|Minor allocation reduction|
|🟢 Low|Short-circuit index building for single-ID lookups|Avoids full dictionary allocation for point queries|
