## Testing instructions
- Use Test Driven Development (TDD)
- Unit tests must pass locally before committing code.
- Use xUnit as the test framework.
- Use NSubstitute for mocking dependencies in unit tests.
- Use Shouldly for assertions in tests.
- Tests should not be skippable. If a test cannot be implemented, raise this as an issue instead of skipping it.
- Tests should never return early to avoid failing assertions.