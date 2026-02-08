# MimironSQL.EntityFrameworkCore

Entity Framework Core database provider for DB2 files.

## Installation

```bash
dotnet add package MimironSQL.EntityFrameworkCore
```

## Configuration

```csharp
var options = new DbContextOptionsBuilder<WoWDb2Context>()
    .UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider)
    .Options;
```

## Supported LINQ

✅ Where, Select, Include, ThenInclude, Take, Skip, First, Single, Count, Any, All  
❌ Async, SaveChanges, GroupBy, Join

## Relationships

```csharp
modelBuilder.Entity<MapChallengeMode>()
    .HasOne(mc => mc.Map)
    .WithMany(m => m.MapChallengeModes)
    .HasForeignKey(mc => mc.MapID);
```

## Change Tracking

```csharp
// Default: tracking enabled
.UseQueryTrackingBehavior(QueryTrackingBehavior.TrackAll)

// Better performance
.UseQueryTrackingBehavior(QueryTrackingBehavior.NoTracking)
```

Target: .NET 10.0  
Dependencies: EF Core 10.0.2
