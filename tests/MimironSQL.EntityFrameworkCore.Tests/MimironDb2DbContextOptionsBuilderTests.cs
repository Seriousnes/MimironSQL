using Microsoft.EntityFrameworkCore;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class MimironDb2DbContextOptionsBuilderTests
{
    [Fact]
    public void Ctor_SetsProtectedOptionsBuilder()
    {
        var optionsBuilder = new DbContextOptionsBuilder();
        var wrapper = new ExposedBuilder(optionsBuilder);

        wrapper.ExposedOptionsBuilder.ShouldBeSameAs(optionsBuilder);
    }

    private sealed class ExposedBuilder(DbContextOptionsBuilder optionsBuilder) : MimironDb2DbContextOptionsBuilder(optionsBuilder)
    {
        public DbContextOptionsBuilder ExposedOptionsBuilder => OptionsBuilder;
    }
}
