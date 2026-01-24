# architecture.md

This document summarizes the high-level layering of the DB2 query engine.

```mermaid
graph TD
    A[LINQ Expression] --> B[Db2Context Query Provider]
    B --> C[Expression Translation]
    C --> D[Query Plan]
    D --> E[Schema Mapper (WoWDBDefs)]
    D --> F[Db2Model (context-scoped)]
    D --> G[WDC5 Reader (binary)]
    G --> H[Raw Stream (FileSystem/CASC)]
```

Notes:

- `Db2Model` is the single source of truth for relationships/navigations.
- Schema mapping provides field/type metadata and default relationship hints; `Db2ModelBuilder` can override/add relationships.
- Execution consumes the plan and requests minimal decoding per source.
