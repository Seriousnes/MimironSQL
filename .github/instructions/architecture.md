# architecture.md

This document summarizes the high-level layering of the DB2 query engine.

```mermaid
graph TD
    A[LINQ Expression] --> B[EF Core query provider]
    B --> C[EF Core query pipeline]
    C --> D[Mimiron EF provider compilation]
    D --> E[Query plan]
    E --> F[Schema mapper (WoWDBDefs)]
    E --> G[Db2Model (context-scoped)]
    E --> H[WDC5 reader (binary)]
    H --> I[Raw stream (FileSystem/CASC)]
```

Notes:

- `Db2Model` is the single source of truth for relationships/navigations.
- Schema mapping provides field/type metadata and default relationship hints; the provider can add/override relationships based on the EF model.
- `Db2ModelBuilder` and related `Db2*Builder` types are internal implementation details and are not part of the public API.
- Execution consumes the plan and requests minimal decoding per source.
