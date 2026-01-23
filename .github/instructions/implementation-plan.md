# wdc5-db2-query-engine-plan.md

## 1. Project Objective

Build a high-performance, read-only query engine capable of querying World of Warcraft **WDC5** client database files via a **LINQ-first / ORM-style API**. The engine must handle complex bit-packing, sparse data structures, and optimized string searching without fully deserializing the entire dataset into objects (zero-allocation/lazy-loading preference).

SQL-text support is a potential later enhancement, but is not a Phase 3 requirement if the LINQ/ORM surface can cover the required query scenarios efficiently.

### DB2 Documentation
All currently available documentation for DB2 file structure is available at https://wowdev.wiki/DB2, including the WDC5 specifications. The workspace includes the C# implementation in `DBCD` and a `WDC5Reader`. 

## 2. Architecture Overview

The system should be layered to separate the raw binary handling from the query logic.

```mermaid
graph TD
    A[LINQ Expression] --> B[Query Provider]
    B --> C[Expression Translator / Predicate Compiler]
    C --> D[Virtual Table Interface]
    D --> E[Schema Mapper (WoWDBDefs)]
    D --> F[WDC5 Reader (Binary)]
    F --> G[Raw Bytes / Stream (FileStream or MemoryStream)]
```

### 2.1 Core Components

1. **WDC5 Reader:** Handles file I/O, header parsing, and raw record extraction.
2. **Column Decoder:** Specialized logic to handle the 4 distinct compression types (None, Bitpacked, Common, Pallet).
3. **Schema Mapper:** Overlays external definition files (`.dbd`) onto raw columns to provide field names and types.
4. **Query Engine:** Translates LINQ queries, optimizes search strategy, and iterates over records.

For Phase 3, the Query Engine is primarily an `IQueryable` provider that translates LINQ expression trees into efficient row predicates and minimal decoding.

---

## 3. Implementation Phases

### Phase 1: The Virtual Table (Reader Layer)

**Goal:** Abstract the complexities of WDC5 so the engine sees a simple grid of data.

* **Memory Strategy:** Use `Stream` or `ReadOnlySpan<byte>` to avoid loading the entire DB2 into the managed heap.
* **Record Access Abstraction:** Implement a strategy pattern for row retrieval to handle the "Dense vs. Sparse" dichotomy transparency.
* *Dense Strategy:* Calculate offset via `Header + (Index * RecordSize)`.
* *Sparse Strategy:* Look up ID in `OffsetMap` to find the exact byte position.

* **Encrypted Sections:** Supported via skip/decrypt policy.
    * If a section has non-zero `tact_key_lookup` and the key is missing (or the section is a placeholder), skip that section.
    * If a key is available, decrypt rows on-demand (per-row) using Salsa20 with nonce = Record ID as little-endian `ulong`.
    * Only hard-fail when no readable sections remain.
* **Sparse Offset Map:** Prefer explicit offsets when present and consistent; otherwise fall back to contiguous sizing. Fail loudly on inconsistent/out-of-range offsets to avoid silent mis-parses.


* **Column Decoding:**
* Implement `IBitReader` to handle non-byte-aligned reads (e.g., reading 11 bits from bit offset 3).
* **Common Data:** If the bit value is 0, return the default value from the `CommonData` block.
* **Pallet Data:** If the value is an index, retrieve the actual value from the `PalletData` array.



### Phase 2: The Schema Mapper

**Goal:** Map generic `Column_0`, `Column_1` to `Name`, `ManaCost`.

* **Input:** `.dbd` files (WoWDBDefs format).
* **Versioning:** Match the `layout_hash` in the WDC5 header to the build definition in the DBD.
* **Mapping Logic:**
* Map DBD "Fields" to WDC5 "Columns".
* *Note:* A single DBD field (e.g., `Loc coordinates[3]`) may map to 3 distinct WDC5 columns. The engine must aggregate these back into arrays if requested.
* Replace heuristic string tests with schema-mapped deterministic assertions once Phase 2 (Schema Mapper) is in place.


### Phase 3: The LINQ Query Provider (ORM Surface)

**Goal:** Execute common queries efficiently via LINQ over `Db2Table<T>` (or similar):

- Filter: `Where(...)`
- Project: `Select(...)`
- Limit: `Take(...)`
- Terminal: `FirstOrDefault()`

The provider should translate supported predicates into per-row predicates to minimize decoding, and materialize entity `T` using schema-backed, case-insensitive field/member mapping.

#### 3.1 The "String Block First" Optimization (Priority Feature)

Standard row scanning is too slow for string searches. Implement the **Reverse String Lookup** strategy (used for supported string predicates such as `Contains`/`StartsWith`/`EndsWith`):

1. **Scan the String Block:** Treat the block as a raw byte array.
2. **Find Matches:** Locate all occurrences of the search term (e.g., "Fire").
3. **Backtrack:** For each match, walk backwards to the previous `\0` (null terminator) to find the **String Start Offset**.
4. **Index Collection:** Collect a `HashSet<int>` of these valid start offsets.
5. **Integer Scan:** When iterating rows, do **not** read/decode the string. Instead, read the **Integer Offset** from the column and check if it exists in the `HashSet`.
* *Performance Gain:* Converts a slow string comparison operation into a fast integer lookup.



#### 3.2 Standard Predicate Logic

For non-string queries (e.g., `ID > 1000`):

1. **Lazy Evaluation:** Do not decode a column unless it is required for the `WHERE` clause.
2. **Short-Circuiting:** If the `WHERE` clause fails on the first column, skip decoding the remaining columns for that row.

Additionally, when possible, compile LINQ predicates into row predicates to avoid materializing entity instances for rows that will be filtered out.

#### 3.3 Virtual Fields and Relations

Phase 3 must support `.dbd`-declared virtual fields exactly as they appear in the schema:

- `$noninline,id$` virtual `ID`: maps to the row's logical primary key (`Wdc5Row.Id`) regardless of dense/sparse.
- `$noninline,relation$` fields: map to WDC5 parent lookup reference IDs per row ("ReferenceData"), enabling filtering/materialization of relation IDs even when the physical column is not inline.

### Phase 4: Relationships, Navigation, and Joins

**Goal:** Support multi-table query scenarios without requiring SQL-text, using schema-declared relations to enable navigation and joins.

- **Navigation properties:** allow entities to reference other tables via relation IDs (e.g., `CollectableSourceQuestSparse.CollectableSourceInfoID` navigates to `CollectableSourceInfo`).
- **Join execution:** support efficient joins between tables (likely starting with inner join) while minimizing decoding.
- **Database context:** support a multi-table context that caches opened tables and shares providers/options.

This phase may introduce additional query operators (e.g., `Join`, `GroupJoin`, `SelectMany`) and/or a dedicated join planner.

### Phase 5: Optional SQL-text Layer (Later Enhancement)

If required, implement SQL-text parsing as a thin layer on top of the LINQ/ORM query engine:

- Parse SQL text into an expression tree and/or an internal query plan.
- Reuse the existing predicate compiler, string-table optimizations, and join planner.

This phase is explicitly optional and should be deferred unless required for interoperability.

---

## 4. Technical Constraints & Data Structures

### 4.1 ID Handling (Primary Key)

* **Dense Mode:** The "ID List" is a separate column. It does not determine the file offset but provides the Primary Key value.
* **Sparse Mode:** The "ID" is derived from the `OffsetMap`.
* *Engine Requirement:* The engine must expose a virtual column `ID` that automatically pulls from the correct source (List vs. Map) based on the WDC5 flags.

### 4.2 Handling Relations (Foreign Keys)

WDC5 files often reference other files (e.g., `Spell` references `SpellDuration`).

* **Phase 3 Scope:** Expose relation IDs and virtual relation fields as numeric columns, so queries can filter and project relation IDs.
* **Phase 4 Scope:** Enable navigation and joins using schema-declared relations (without requiring SQL-text).

### 4.3 Data Types

The engine must map internal WDC5 types to SQL-compatible types:

* `int` (1-64 bit packed) -> `Int64`
* `float` -> `Single/Double`
* `string` (offset) -> `String` (resolved lazily)

---

## 5. Task List for Agents

1. **Boilerplate:** Create structs for `WDC5_Header`, `Section_Header`, and `Field_Storage_Info`.
2. **BitReader:** Implement a highly optimized `BitReader` capable of reading X bits from a `ReadOnlySpan<byte>`.
3. **StorageParsers:** Write unit tests for each Compression Type (0-4) using known hex snippets from actual WDC5 files.
4. **OffsetMap:** Implement the parser for the sparse offset map section.
5. **StringScanner:** Implement the "Backtrack to Null" search algorithm for the String Block.
6. **LINQ Provider:** Implement an `IQueryable` provider that translates expression trees to row predicates and materializes entities.
7. **Joins/Navigation:** Build join and navigation support using schema-declared relations.
8. **Optional SQL-text:** If needed, add a SQL-text layer that compiles to the same underlying query plan.

## Progress Log
(keep this updated as phases and steps are updated)

### Phase 1
- Started on branch `feature/phase-1-virtual-table`
- Implemented sparse offset map correctness checks (prefer explicit offsets, otherwise contiguous sizes)
- Implemented per-row encryption support (skip encrypted sections when key missing; decrypt on-demand when key present)

### Phase 2
- Started on branch `feature/phase-2-schema-mapper`
- Added `.dbd`-backed `SchemaMapper` that resolves `layout_hash` and returns field names + types + column spans (including virtual non-inline fields)
- Added `IDbdProvider` + filesystem implementation (tableName -> `{tableName}.dbd`)
- Fixed `.dbd` parsing for "many BUILD lines, one entry block" layouts (e.g., ActionBarGroupEntry)
- Replaced string heuristic tests with schema-backed deterministic tests using in-repo `.dbd` fixtures
- Corrected `.dbd` array semantics (`[n]` is in-field element count, not extra physical columns) and ignored `COMMENT` lines
- Added relation/reference metadata to schema (`IsRelation`, `ReferencedTableName`)
- Added `IsVerified` semantics for trailing `?` in `COLUMNS` (unverified columns, not nullable)
- Added real-pair coverage for `AccountStoreCategory` (inline `ID`, relation `StoreFrontID`)

### Phase 3
- Started on branch `feature/phase-3-query-engine`
- Added LINQ-first query surface (`Db2Database` + `Db2Table<T>`) targeting ORM-style usage (entities/materialization, not SQL text)
- Implemented translation of `Where` predicates to row predicates for minimal decoding; supports comparisons + string `Contains`/`StartsWith`/`EndsWith`
- Implemented dense string-table scanning optimization (string-block-first) when applicable
- Surfaced parent lookup reference IDs per row and used them to materialize/query `$noninline,relation$` virtual fields
- Added fixture-based Phase 3 tests covering all existing `.db2` fixtures
