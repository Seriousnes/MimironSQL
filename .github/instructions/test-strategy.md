## Testing instructions
- **IMPORTANT** if tests are skipped because the build fails, immediately stop trying to run tests and fix the build errors
- Use Test Driven Development (TDD)
- Unit tests must pass locally before committing code.
- Use xUnit as the test framework.
- Use NSubstitute for mocking dependencies in unit tests.
- Use Shouldly for assertions in tests.
- Tests should not be skippable. If a test cannot be implemented, raise this as an issue instead of skipping it.
- Tests should never return early to avoid failing assertions.

## Test project structure

- Unit tests are split per library under `Tests/` (e.g. `Tests/MimironSQL.Dbd.Tests`, `Tests/MimironSQL.Formats.Wdc5.Tests`, etc.).
- Integration tests live in `Tests/MimironSQL.Integration.Tests` and are intended to cover multi-project scenarios (query pipeline execution, generated DbContext usage, provider + format interactions).
- CASC integration scenarios are **local-only** and must not be required for CI to pass.

## Coverage (collector-based)

- Coverage is collected via the VSTest collector (Coverlet collector), not MSBuild instrumentation:
	- `dotnet test <test-project> --collect:"XPlat Code Coverage" --settings coverlet.runsettings`
- CI merges all produced `**/coverage.cobertura.xml` into a single Cobertura report and enforces a **90% line coverage** gate.
- Exclusions are configured in `coverlet.runsettings`:
	- Excludes Salsa20 by assembly filter (`[Salsa20*]*`).
	- Excludes generated/build artifacts by file patterns (e.g. `**/*.g.cs`, `**/*AssemblyAttributes.cs`, `**/obj/**`, `**/bin/**`).

## Heuristic tests (DB2 parsing)
- Prefer deterministic assertions over heuristics (e.g., known IDs/values via schema mapping).
- If a heuristic is unavoidable (pre-schema-mapper), it must be designed to *fail loudly* when core math is wrong:
	- Require multiple independent hits (e.g., 5+ distinct decoded strings), not a single match.
	- Constrain candidates (length bounds, non-whitespace, contains at least one letter/digit) to reduce accidental successes.
	- Prefer separate tests for dense string-table decoding vs sparse inline-string decoding so the source is explicit.