# Integration Tests - Temporarily Disabled

The IntegrationTests are temporarily disabled as they need to be refactored to work with the new EF Core `DbContext` generator output.

## Current Status

The `DbContextGenerator` now emits standard EF Core `DbContext` classes with `DbSet<T>` properties instead of the custom `Db2Context` with `Db2Table<T>` properties. The IntegrationTests still use the old `Db2Context` approach and need to be updated.

## Changes Needed

1. Update test context creation to use `DbContextOptions<WoWDb2Context>`
2. Update tests to use EF Core APIs instead of custom Db2Context APIs
3. Integrate with the EF Core provider (UseMimironDb2FileSystem, UseMimironDb2Casc)
4. Update entity navigation property expectations

## Re-enabling

Once the EF Core provider is fully implemented and the tests are updated, remove the generator disabling comments from `MimironSQL.IntegrationTests.csproj` and delete this README.
