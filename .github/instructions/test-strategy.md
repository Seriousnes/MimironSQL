## Testing instructions
- **IMPORTANT** if tests are skipped because the build fails, immediately stop trying to run tests and fix the build errors
- Use Test Driven Development (TDD)
- Unit tests must pass locally before committing code.
- Use xUnit as the test framework.
- Use NSubstitute for mocking dependencies in unit tests.
- Use Shouldly for assertions in tests.
- Tests should not be skippable. If a test cannot be implemented, raise this as an issue instead of skipping it.
- Tests should never return early to avoid failing assertions.

## Heuristic tests (DB2 parsing)
- Prefer deterministic assertions over heuristics (e.g., known IDs/values via schema mapping).
- If a heuristic is unavoidable (pre-schema-mapper), it must be designed to *fail loudly* when core math is wrong:
	- Require multiple independent hits (e.g., 5+ distinct decoded strings), not a single match.
	- Constrain candidates (length bounds, non-whitespace, contains at least one letter/digit) to reduce accidental successes.
	- Prefer separate tests for dense string-table decoding vs sparse inline-string decoding so the source is explicit.