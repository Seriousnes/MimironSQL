# db2-format.md

This document is a home for DB2/WDC5 format details and parsing constraints so the implementation plan can stay focused on remaining work.

## References

- WDC/DB2 overview: https://wowdev.wiki/DB2

## Primary key / ID handling

- Dense mode: the primary key values come from the ID list (not from file offsets).
- Sparse mode: the row ID is derived from the sparse offset map.
- Engine requirement: expose a virtual `ID` field that reads from the correct source depending on file flags.

## Relations / foreign keys

- DB2/DBD relations may be expressed as schema metadata (e.g., `int<Map::ID> ParentMapID` via `ReferencedTableName`).
- WDC5 also supports `$noninline,relation$` virtual fields which map to per-row reference IDs ("ReferenceData").

## Data types

The engine maps WDC5 types to CLR/SQL-friendly types:

- Integers (packed 1–64 bits): `Int64` (or narrower when safe)
- Floats: `Single` (and `Double` only when required by semantics)
- Strings: integer string offsets, resolved lazily to `String`

## Row storage and record access

- Records may be stored densely or sparsely per section.
- Encrypted sections may be skipped or decrypted on-demand depending on available keys/policy.

## String storage

- Many tables use a dense string table (null-terminated strings) referenced by integer offsets.
- Optimizations may scan the string block directly and translate to offset/ID sets for semi-join execution.
