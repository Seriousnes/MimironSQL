# WIP Plan: Iterative EF Core Provider Bootstrap (DB2)

## Status
- **Work in progress**: the provider is being rebuilt from scratch.
- **Goal**: incremental, correctness-first progress.
- **Non-goal (for now)**: having all tests pass immediately.

## North Star
Build a real EF Core database provider (read-only) for querying DB2 data via EF Core’s normal query pipeline patterns (Cosmos/InMemory-style), while keeping the codebase easy to evolve and optimize later.

## Operating Principles
- **Fix one failure at a time**: always address the *earliest* failing error surfaced by the test suite/run.
- **Smallest viable change**: add only the minimum code/services needed to eliminate the current error and progress to the next.
- **Correctness over performance (initially)**: prefer a simple correct execution path over early micro-optimizations.
- **Continuously simplify**: if a newly-added component becomes unused, remove it aggressively; restore later only if a later failing test proves it is needed.
- **Follow EF Core conventions**: mirror the patterns used by existing EF Core providers (e.g., Cosmos/InMemory) rather than inventing new ones.
- **No binary/DLL scanning**: use public EF Core source and provider examples for discovery and alignment.
- **Default to Cosmos patterns**: before asking a follow-up question, check how EF Core Cosmos implements the same concern; only ask if still unclear.

## Iteration Loop (repeat until the suite progresses)
1. **Run a focused test command** that reproduces the current earliest failure.
2. **Identify the first/root exception** (ignore secondary cascades).
3. **Classify the failure**:
   - Provider configuration / service wiring
   - Model shaping / conventions
   - Query translation pipeline
   - Query execution pipeline
   - Materialization / tracking / fix-up
   - Includes / relationship behaviors
4. **Implement the smallest fix** to remove *that* specific failure.
5. **Refactor immediately if warranted**:
   - Keep interfaces clean
   - Avoid leaking internal-only types
   - Remove unused/temporary code
6. **Re-run the same test command** to ensure the failure moved forward.
7. **Record learning**: update this plan briefly if the overall direction changes.

## Progressive Milestones (conceptual)
- **M0: Provider is recognized & bootstraps**
  - Context can be constructed, model can be built, and a query can compile far enough to surface the next missing piece.

- **M1: Minimal query pipeline compiles**
  - Queries translate/compile end-to-end (even if the execution path is simplistic).

- **M2: Minimal execution produces results**
  - Basic projections and filtering return correct results.

- **M2.5: Native DB2 execution for core operators**
  - Prefer native DB2 ops for `Where`, `Find`, and `Include` to minimize table reads, while staying inside EF Core conventions.
  - See `plans/plan-query-execution.md`.

- **M3: Navigation behaviors become EF-native**
  - Includes/relationships work via EF conventions and provider execution (not custom/manual include engines).

- **M4: Expand translation coverage**
  - Add support for additional LINQ patterns as tests demand.

## Guardrails
- **Keep changes surgical**: don’t fix unrelated failures unless they block forward progress.
- **Prefer provider-local implementations**: avoid coupling to EF internals unless the provider patterns clearly require it.
- **Document intent, not mechanics**: focus on *why* something exists so it can be removed later if it stops being necessary.

## Definition of “Good Progress”
- The earliest failure changes (moves forward) after each iteration.
- New code is minimal, cohesive, and removable.
- The provider approaches EF Core provider conventions over time.

## Current Clarifications (2026-02-14)
- **Async query support deferred**: queries are sync-only for now; async integration tests may be skipped temporarily.
- **Correctness-first execution**: it is acceptable to execute some predicates client-side initially.
  - TODO in code: switch to DB2-native filtering/selection later.
- **Native DB2 ops focus (2026-02-15)**: prioritize native DB2 execution for `Where`/`Find`/`Include` to reduce reads.
  - Allow client-side evaluation for arbitrary .NET methods/complex constructs *after* safe pushdown where possible.
  - Safe partial pushdown rule of thumb: allow partial pushdown for `&&`, avoid unsafe partial pushdown under `||`.
  - Plan: `plans/plan-query-execution.md`.
- **Navigation predicate translation is a milestone**: avoid long-term dependence on compiling EF Core rewritten lambdas.
  - TODO: translate navigation predicates (e.g., `.Any`, `.Count`, `.Contains` over navigations) into joins/subqueries in the provider IR.
- **DI pattern**: follow Cosmos style (provider registers EF interfaces, then uses provider-specific services via DI).
