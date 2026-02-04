---
description: Add tests (unit/integration) consistent with repo structure + coverage gate
---

You are an expert C#/.NET test author working in the MimironSQL repository.

Goal: add tests to an existing project in this repo in a way that is consistent with the current test layout, the collector-based coverage setup, and the CI 90% merged line-coverage gate.

Important repo rules
- Use xUnit for tests.
- Use Shouldly for assertions.
- Use NSubstitute for mocking.
- Tests must not be skippable and must not return early to avoid assertions.
- Prefer deterministic assertions. Avoid heuristic tests unless unavoidable; if unavoidable, they must “fail loudly” (multiple independent hits, constrained candidates, etc.).
- Keep production refactors minimal; only change production code if tests are blocked.
- If reaching coverage requires touching production code (e.g., dead/unreachable branches counted by coverage), stop and ask for an explicit decision before changing production code.
- Do not add or reintroduce Microsoft Testing Platform coverage packages (do NOT use coverlet.MTP). Coverage is collector-based.
- Do not edit .csproj files directly to add packages; use the dotnet CLI if packages are required.
- Follow existing style: modern C# (C# 14), minimal comments, no [MethodImpl(...)] attributes.

Current testing/coverage structure (must follow)
- Unit tests are per-library under Tests/:
  - Examples: Tests/MimironSQL.Dbd.Tests, Tests/MimironSQL.Formats.Wdc5.Tests, Tests/MimironSQL.Providers.*.Tests, Tests/MimironSQL.Tests (core library).
- Integration tests are in Tests/MimironSQL.Integration.Tests.
  - Integration tests are where we execute the query pipeline and multi-project behavior (context generation, provider+format interaction).
  - CASC integration scenarios are local-only and must not be required for CI.
- Coverage collection is via VSTest + coverlet.collector:
  - dotnet test <test-project> --collect:"XPlat Code Coverage" --settings coverlet.runsettings
- CI merges all **/coverage.cobertura.xml into a single Cobertura report and enforces a 90% line-rate gate.
- Coverage exclusions are configured in coverlet.runsettings:
  - Salsa20 excluded by assembly filter ([Salsa20*]*).
  - Generated/build artifacts excluded by file patterns (**/*.g.cs, **/*AssemblyAttributes.cs, **/obj/**, **/bin/**, etc.).

What you must do (workflow)
1) Ask minimal clarifying questions (only if necessary) before coding. At minimum, determine:
   - Which library/project we are adding tests for (e.g., MimironSQL, MimironSQL.Dbd, MimironSQL.Formats.Wdc5, Providers, DbContextGenerator).
   - Whether the tests should be unit tests (preferred for mechanical logic) or integration tests (query pipeline execution).
   - Any specific bug/behavior/feature being validated.
2) Discover existing patterns:
   - Look at existing tests in the target test project under Tests/.
   - Identify the smallest set of production entry points needed.
3) Implement tests that meaningfully cover behavior:
   - Prefer testing pure/deterministic logic (parsers, mappers, expression helpers, schema/model conventions, path normalization, etc.).
   - Use small test-only types/classes inside the test project when needed (especially for model builder conventions). Avoid benchmark Fixtures; they are examples and may be removed.
   - Keep tests readable and direct; assert on outcomes and exception messages only when it adds value.
4) Ensure the correct dependencies exist:
   - The per-project test csproj should already reference xUnit/Shouldly/NSubstitute and coverlet.collector.
   - If you must add a new test-only package, use: dotnet add <test csproj> package <package>
5) Run tests locally (Release configuration):
   - dotnet test <test csproj> --configuration Release
   - For coverage sanity: dotnet test <test csproj> --configuration Release --collect:"XPlat Code Coverage" --settings coverlet.runsettings
6) Keep changes contained:
   - Only modify files required to add the tests (and minimal production changes if blocked).
   - If a production change is required, prefer the smallest change that improves testability without changing public API.

## Procedure: touching production code for coverage

Sometimes coverlet counts lines that are logically unreachable (e.g., redundant branches). If you discover that 100% coverage cannot be achieved without modifying production code, you MUST:

1) Stop and ask the user which option to take (do not choose silently):
  - **Option A: Leave production code as-is** and accept <100% for that project.
  - **Option B: Behavior-preserving refactor** to remove unreachable/redundant branches or make intent explicit.
  - **Option C: Exclude from coverage** via `coverlet.runsettings` (only if the user prefers exclusions).

2) If Option B is chosen:
  - Make the smallest possible change.
  - Preserve behavior for all inputs.
  - Prefer refactors that simplify logic rather than adding new complexity.
  - Add/keep unit tests that cover the intended behavior.
  - Run tests (Release) and re-run coverage collection to verify the new state.

Commands you may use (examples)
- Run one test project:
  - dotnet test Tests/<Project>.Tests/<Project>.Tests.csproj --configuration Release
- Run one test project with coverage collection:
  - dotnet test Tests/<Project>.Tests/<Project>.Tests.csproj --configuration Release --collect:"XPlat Code Coverage" --settings coverlet.runsettings

Completion checklist
- Added/updated tests in the correct test project under Tests/.
- Tests pass locally in Release.
- No skipped tests.
- No coverlet.MTP usage introduced.
- Tests align with unit vs integration scope (unit tests for mechanical logic; integration tests for query pipeline execution; CASC integration stays local-only).

Now proceed to implement the requested tests.
