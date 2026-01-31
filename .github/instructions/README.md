# MimironSQL Implementation Instructions

This directory contains design documents and implementation guidance for the MimironSQL DB2 query engine.

## Quick Reference

### Primary Documents

- **[implementation-plan.md](./implementation-plan.md)** — Overall project roadmap and phased implementation breakdown (Phases 1–6)
- **[architecture.md](./architecture.md)** — High-level system architecture and component relationships
- **[coding-style.md](./coding-style.md)** — C# coding conventions for this project
- **[test-strategy.md](./test-strategy.md)** — Testing approach and requirements

### Technical References

- **[db2-format.md](./db2-format.md)** — DB2/WDC5 binary format notes and parsing details
- **[query-engine-notes.md](./query-engine-notes.md)** — Query translation and execution strategy notes

### Phase-Specific Design Documents

- **[phase-6-design.md](./phase-6-design.md)** — Detailed design for Phase 6 (collection navigations, multi-hop navigations, SQL-text layer)
- **[phase-6-model-extensions.md](./phase-6-model-extensions.md)** — Model class designs for Phase 6 collection and multi-hop navigation metadata

## Implementation Phases

### Completed Phases

- ✅ **Phase 1** — `Db2Model` foundation (single source of truth for metadata)
- ✅ **Phase 2** — `Db2ModelBuilder` configuration surface (OnModelCreating, IDb2EntityTypeConfiguration)

### In-Progress / Planned Phases

- 🚧 **Phase 3** — Navigation-aware expression translation (broader predicate shapes, scalar comparisons)
- 🚧 **Phase 4** — Multi-source execution semantics and robustness (no silent fallbacks)
- 🚧 **Phase 5** — Navigation projections (avoid root materialization, improve pruning)
- 📋 **Phase 6** — Optional/Later (collection navigations, multi-hop, SQL-text layer) — *Blocked by Phases 3–5*

See [implementation-plan.md](./implementation-plan.md) for detailed phase descriptions and acceptance criteria.

## Getting Started

If you're contributing to MimironSQL:

1. Read [implementation-plan.md](./implementation-plan.md) to understand the project roadmap
2. Review [coding-style.md](./coding-style.md) for C# conventions
3. Consult [test-strategy.md](./test-strategy.md) before adding tests
4. Check the relevant phase design document for implementation guidance

For Phase 6 specifically, start with [phase-6-design.md](./phase-6-design.md) — but note that it is blocked by Phases 3–5 and should not be implemented until those phases are complete and stable.
