using Shouldly;

namespace MimironSQL.DbContextGenerator.Tests;

public sealed class DbContextGeneratorSmokeTests
{
    [Fact]
    public void Generator_can_be_constructed()
    {
        var generator = new MimironSQL.DbContextGenerator.DbContextGenerator();
        generator.ShouldNotBeNull();
    }
}
