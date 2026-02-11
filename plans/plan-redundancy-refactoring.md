# Plan: Query Compiler Stream & Metadata Acquisition Refactoring

## Problem Statement

The query execution pipeline acquires streams and metadata in an ad-hoc, scattered manner. Different code paths independently open streams, parse file metadata, and build schemas — sometimes multiple times for the same table within a single query. The query compiler should centralize this upfront.

### Current Flow (Full-Scan Path)
1. `MimironDb2QueryExecutor.ExecuteTyped` calls `_store.OpenTableWithSchema<TRow>(tableName)`
2. `MimironDb2Store.OpenTableWithSchema` lazy-caches a `(IDb2File, Db2TableSchema)` per table name
3. Inside the cache factory: opens one stream via `OpenDb2Stream`, calls `_format.OpenFile(stream)` (which fully parses the file), then `_format.GetLayout(file)` + `_schemaMapper.GetSchema(key, layout)`
4. The `IDb2File` and schema are passed to `Db2QueryProvider`, which uses them for enumeration

### Current Flow (Key-Lookup Path)
1. `MimironDb2QueryExecutor.ExecuteTyped` calls `_store.TryMaterializeById<TEntity>(tableName, id, ...)`
2. `MimironDb2Store.TryMaterializeById` lazy-caches a `KeyLookupTable` per table name
3. Inside the cache factory: opens stream #1 via `OpenDb2Stream`, calls `Wdc5KeyLookupMetadata.Parse(stream)` (parses header + section metadata + id index, skips record data), then gets schema
4. `TryResolveRowHandle` resolves the ID to a section/row location
5. Opens stream #2 via `OpenDb2Stream` to create a `Wdc5KeyLookupRowFile` that reads just one row
6. `Db2EntityMaterializer.Materialize(rowFile, handle)` reads fields from the single-row file

### What's Wrong
- **Two separate caches** (`_cache` and `_keyLookupCache`) hold overlapping-but-incompatible metadata for the same tables
- **Two separate streams** are opened for a single key-lookup query — one for metadata, one for row data
- **The query compiler defers all I/O to the store**, meaning it cannot reason about or optimize cross-table access patterns
- **Include/navigation queries** open additional streams per related table, each independently cached
- **Schema-from-metadata** (`GetSchemaFromMetadata`) is a third path that opens yet another stream just to read the 200-byte header, using the hard-coded `Wdc5LayoutReader`

## Proposed Changes

### Step 1: Introduce a `QuerySession` concept
Before executing any query, `MimironDb2QueryExecutor` should analyze the expression to determine **all tables** involved (primary entity + any Include navigations). It should then acquire all necessary resources upfront in a single pass:

```
QuerySession {
    TableInfo[] Tables;  // one per table involved in query
}

TableInfo {
    string TableName;
    IDb2File File;           // lazily opened, but only once
    Db2TableSchema Schema;
    // Future: id-index for key-lookup support
}
```

### Step 2: Eliminate the three-cache pattern in MimironDb2Store
Currently there are:
- `_cache` → `ConcurrentDictionary<string, Lazy<(IDb2File, Db2TableSchema)>>` (full-scan path)
- `_schemaFromMetadataCache` → `ConcurrentDictionary<string, Lazy<Db2TableSchema>>` (schema-only path)
- `_keyLookupCache` → `ConcurrentDictionary<string, Lazy<KeyLookupTable>>` (key-lookup path)

These should be unified into a single cache that holds a table descriptor capable of serving both full-scan and key-lookup needs. The `IDb2File` instance (once `Wdc5File` is consolidated per the DRY plan) should support both enumeration and id-based lookup from the same instance.

### Step 3: Single stream acquisition per table per query
A table should require at most **one** `OpenDb2Stream` call per query:
- For full-scan: open once, parse file, enumerate rows
- For key-lookup: open once, parse file (with lazy record blob loading — see DRY plan), resolve id, read row bytes

This is achievable once `Wdc5File` supports lazy record data loading — the id-index can be built from section metadata without reading record bytes.

### Step 4: Move table resolution out of the store
The store should expose a method like:
```csharp
IDb2File OpenFile(string tableName);
Db2TableSchema GetSchema(string tableName);
```
And the query executor should coordinate the session, rather than having the store internally decide between full-scan vs key-lookup strategies.

### Step 5: Pre-compute multi-table access plans
For Include queries, the query executor currently:
1. Opens the primary table
2. Materializes primary entities one-by-one
3. For each entity, resolves the navigation's FK
4. Opens the related table (cached, but still lazy)
5. Looks up related entities

Instead, the session should pre-open all tables, so navigation resolution doesn't trigger additional stream opens mid-enumeration.

## Dependencies
- **Plan: DRY Violations** — The unified cache requires `Wdc5File` to support both enumeration and key-lookup from a single instance.
- **Plan: Tight Coupling Removal** — The session/store layer must work through `IDb2Format`/`IDb2File` abstractions.

## Acceptance Criteria
- [ ] A single key-lookup query opens at most one stream per table
- [ ] Full-scan and key-lookup share the same table cache entry
- [ ] Include queries pre-resolve all table resources before enumeration begins
- [ ] `GetSchemaFromMetadata` is removed or consolidated into the single cache
- [ ] All existing integration tests pass unchanged
