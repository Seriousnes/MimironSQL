## Testing instructions
- **IMPORTANT** if tests are skipped because the build fails, immediately stop trying to run tests and fix the build errors
- Use Test Driven Development (TDD)
- Unit tests must pass locally before committing code.
- Use xUnit as the test framework.
- Use NSubstitute for mocking dependencies in unit tests.
- Use Shouldly for assertions in tests.
- Tests should not be locally skippable, however CASC-based tests may be skipped in CI/CD workflows.
- Tests should never return early to avoid failing assertions.

## Test project structure

- Unit tests are split per library under `tests/` (e.g. `tests/MimironSQL.Dbd.Tests`, `tests/MimironSQL.Formats.Wdc5.Tests`, etc.).
- Integration tests live in `tests/MimironSQL.IntegrationTests` and are intended to cover multi-project scenarios (query pipeline execution, generated `DbContext` usage, provider + format interactions).
- CASC integration scenarios are **local-only** and must not be required for CI to pass.

## Coverage (collector-based)

- Coverage is collected via the VSTest collector (Coverlet collector), not MSBuild instrumentation:
    - `dotnet test <test-project> --collect:"XPlat Code Coverage" --settings tests/coverlet.runsettings`
- CI merges all produced `**/coverage.cobertura.xml` into a single Cobertura report and enforces a **90% line coverage** gate.
- Exclusions are configured in `coverlet.runsettings`:
	- Excludes Salsa20 by assembly filter (`[Salsa20*]*`).
	- Excludes generated/build artifacts by file patterns (e.g. `**/*.g.cs`, `**/*AssemblyAttributes.cs`, `**/obj/**`, `**/bin/**`).

## Coverage artifacts (local workflow)

Local coverage runs must be deterministic and must not create untracked artifact directories.

Rules:
- All local coverage artifacts live under the repo root `coverage/` directory.
- Keep history: each run goes into `coverage/history/<timestamp>/`.
- Never write `TestResults/` directories under `tests/` or anywhere else.
  - Always pass `--results-directory` to `dotnet test`.
- ReportGenerator output must also go under the same `coverage/history/<timestamp>/` run directory.

### Canonical PowerShell command (single command)

Run from the repo root:

```powershell
$ts = Get-Date -Format 'yyyyMMdd-HHmmss'
$run = Join-Path $PWD "coverage\history\$ts"
$results = Join-Path $run 'testresults'
$merged = Join-Path $run 'merged'

New-Item -ItemType Directory -Force -Path $results, $merged | Out-Null

dotnet test .\MimironSQL.slnx --configuration Release --collect:"XPlat Code Coverage" --settings .\tests\coverlet.runsettings --results-directory $results

reportgenerator -reports:"$results\**\coverage.cobertura.xml" -targetdir:$merged -reporttypes:"Cobertura;TextSummary"
Get-Content "$merged\Summary.txt" -Raw
```

### Ranking missed coverage (reportcoverage.cs)

`reportcoverage.cs` usage is documented in [tools/coverage/README.md](../../tools/coverage/README.md).

### One-time cleanup (if stray artifacts exist)

```powershell
Remove-Item -Recurse -Force .\coverage-merged-* -ErrorAction SilentlyContinue
Get-ChildItem . -Recurse -Directory -Filter TestResults | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
```

## Heuristic tests (DB2 parsing)
- Prefer deterministic assertions over heuristics (e.g., known IDs/values via schema mapping).
- If a heuristic is unavoidable (pre-schema-mapper), it must be designed to *fail loudly* when core math is wrong:
	- Require multiple independent hits (e.g., 5+ distinct decoded strings), not a single match.
	- Constrain candidates (length bounds, non-whitespace, contains at least one letter/digit) to reduce accidental successes.
	- Prefer separate tests for dense string-table decoding vs sparse inline-string decoding so the source is explicit.

## DRY (Don't Repeat Yourself)
- When multiple tests with a test class need to repeat the same setup, use xunit's `IClassFixture<T>` for sharing context between tests. E.g. 
    ❌ Don't
    ```csharp
        [Fact]
        public void Test_A()
        {
            var testDataDir = TestDataPaths.GetTestDataDirectory();
            var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
            var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
            var tactKeyProvider = Substitute.For<ITactKeyProvider>();
            tactKeyProvider.TryGetKey(Arg.Any<ulong>(), out Arg.Any<ReadOnlyMemory<byte>>()).Returns(false);

            var options = new DbContextOptionsBuilder<WoWDb2Context>()
                .UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider)
                .Options;

            var context = new WoWDb2Context(options);

            var results = context.Spell
                .Include(s => s.SpellName)
                .Select(s => new { s.Id, Name = s.SpellName.Name_lang })
                .ToList();

            results.Count.ShouldBeGreaterThan(0);
        }

        [Fact]
        public void Test_B()
        {
            var testDataDir = TestDataPaths.GetTestDataDirectory();
            var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
            var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
            var tactKeyProvider = Substitute.For<ITactKeyProvider>();
            tactKeyProvider.TryGetKey(Arg.Any<ulong>(), out Arg.Any<ReadOnlyMemory<byte>>()).Returns(false);

            var options = new DbContextOptionsBuilder<WoWDb2Context>()
                .UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider)
                .Options;

            var context = new WoWDb2Context(options);

            var categories = context.AccountStoreCategory;

            var first = categories
                .Where(x => x.StoreFrontID > 0)
                .FirstOrDefault();

            first.ShouldNotBeNull();
            first!.Id.ShouldBeGreaterThan(0);
        }
    ```

    ✅ Do
    ```csharp
    public class FileSystemProviderQueryTests(TestFixture fixture) : IClassFixture<TestFixture>
    {
        [Fact]
        public void Test_A()
        {
            var results = fixture.Context.Spell
                .Include(s => s.SpellName)
                .Select(s => new { s.Id, Name = s.SpellName.Name_lang })
                .ToList();

            results.Count.ShouldBeGreaterThan(0);
        }

        [Fact]
        public void Test_B()
        {
            var categories = fixture.Context.AccountStoreCategory;

            var first = categories
                .Where(x => x.StoreFrontID > 0)
                .FirstOrDefault();

            first.ShouldNotBeNull();
            first!.Id.ShouldBeGreaterThan(0);
        }
    }

    internal sealed class TestFixture
    {
        public TestFixture()
        {
            var testDataDir = TestDataPaths.GetTestDataDirectory();

            var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
            var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

            var tactKeyProvider = Substitute.For<ITactKeyProvider>();
            tactKeyProvider.TryGetKey(Arg.Any<ulong>(), out Arg.Any<ReadOnlyMemory<byte>>()).Returns(false);

            var options = new DbContextOptionsBuilder<WoWDb2Context>()
                .UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider)
                .Options;

            Context = new WoWDb2Context(options);
        }

        public WoWDb2Context Context { get; }
    }
    ```
- Static helper methods to create a context are not an acceptable alternative to the above fixture pattern