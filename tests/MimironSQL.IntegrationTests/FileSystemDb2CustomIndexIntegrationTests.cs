using MimironSQL.IntegrationTests.Helpers;

using Shouldly;

namespace MimironSQL.IntegrationTests;

public sealed class FileSystemDb2CustomIndexIntegrationTests(FileSystemTextFixture fixture) : IClassFixture<FileSystemTextFixture>
{
    private WoWDb2Context context => fixture.Context;

    [Fact]
    public void Equality_query_on_numeric_scalar_builds_custom_index_and_returns_matches()
    {
        var sample = context.MapChallengeMode
            .OrderBy(x => x.Id)
            .Take(512)
            .Select(x => new { x.Id, x.Flags })
            .ToList();

        sample.Count.ShouldBeGreaterThan(0);

        var targetGroup = sample
            .GroupBy(x => x.Flags)
            .OrderByDescending(x => x.Count())
            .First();

        var targetFlags = targetGroup.Key;
        var expectedSampleIds = targetGroup.Select(x => x.Id).ToHashSet();

        var wowVersionDirectory = Path.Combine(fixture.IndexCacheDirectory, TestHelpers.WowVersion);
        var indexFilesBefore = Directory.Exists(wowVersionDirectory)
            ? Directory.GetFiles(wowVersionDirectory, "MapChallengeMode_Flags_*.db2idx", SearchOption.TopDirectoryOnly)
            : [];

        indexFilesBefore.Length.ShouldBe(0);

        var results = context.MapChallengeMode
            .Where(x => x.Flags == targetFlags)
            .ToList();

        results.Count.ShouldBeGreaterThanOrEqualTo(expectedSampleIds.Count);
        results.Select(static x => x.Id).ToHashSet().IsSupersetOf(expectedSampleIds).ShouldBeTrue();

        Directory.Exists(wowVersionDirectory).ShouldBeTrue();
        Directory.GetFiles(wowVersionDirectory, "MapChallengeMode_Flags_*.db2idx", SearchOption.TopDirectoryOnly)
            .Length.ShouldBeGreaterThan(0);
    }
}
