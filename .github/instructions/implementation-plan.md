# wdc5-db2-sql-engine-plan.md

## 1. Project Objective

Build a high-performance, read-only SQL engine capable of querying World of Warcraft **WDC5** client database files. The engine must handle complex bit-packing, sparse data structures, and optimized string searching without fully deserializing the entire dataset into objects (zero-allocation/lazy-loading preference).

### DB2 Documentation
All currently available documentation for DB2 file structure is available at https://wowdev.wiki/DB2, including the WDC5 specifications. The workspace includes the C# implementation in `DBCD` and a `WDC5Reader`. 

## 2. Architecture Overview

The system should be layered to separate the raw binary handling from the query logic.

```mermaid
graph TD
    A[SQL Query] --> B[Query Parser]
    B --> C[Execution Planner]
    C --> D[Virtual Table Interface]
    D --> E[Schema Mapper (WoWDBDefs)]
    D --> F[WDC5 Reader (Binary)]
    F --> G[Raw Bytes / Stream (FileStream or MemoryStream)]
```

### 2.1 Core Components

1. **WDC5 Reader:** Handles file I/O, header parsing, and raw record extraction.
2. **Column Decoder:** Specialized logic to handle the 4 distinct compression types (None, Bitpacked, Common, Pallet).
3. **Schema Mapper:** Overlays external definition files (`.dbd`) onto raw columns to provide field names and types.
4. **Query Engine:** Parses SQL, optimizes search strategy, and iterates over records.

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


### Phase 3: The Query Engine (Execution)

**Goal:** Execute `SELECT * FROM Spells WHERE Name LIKE '%Fire%'` efficiently.

#### 3.1 The "String Block First" Optimization (Priority Feature)

Standard row scanning is too slow for string searches. Implement the **Reverse String Lookup** strategy:

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

---

## 4. Technical Constraints & Data Structures

### 4.1 ID Handling (Primary Key)

* **Dense Mode:** The "ID List" is a separate column. It does not determine the file offset but provides the Primary Key value.
* **Sparse Mode:** The "ID" is derived from the `OffsetMap`.
* *Engine Requirement:* The engine must expose a virtual column `ID` that automatically pulls from the correct source (List vs. Map) based on the WDC5 flags.

### 4.2 Handling Relations (Foreign Keys)

WDC5 files often reference other files (e.g., `Spell` references `SpellDuration`).

* **Initial Scope:** Do not implement automatic JOINs. Require explicit IDs.
* **Future Scope:** Allow loading multiple DB2s into a `DataSet` context to enable cross-file querying.

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
6. **SQL Parser:** Integrate a lightweight SQL parser (e.g., `SqlParser-cs` or a custom Regex-based parser for MVP) to translate text to Expression Trees.

## Progress Log
(keep this updated as phases and steps are updated)

### Phase 1
- Started on branch `feature/phase-1-virtual-table`
- Implemented sparse offset map correctness checks (prefer explicit offsets, otherwise contiguous sizes)
- Implemented per-row encryption support (skip encrypted sections when key missing; decrypt on-demand when key present)