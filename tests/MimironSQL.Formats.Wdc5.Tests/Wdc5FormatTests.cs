using Shouldly;

namespace MimironSQL.Formats.Wdc5.Tests;

public sealed class Wdc5FormatTests
{
    [Fact]
    public void Register_throws_for_null_registry()
    {
        Should.Throw<ArgumentNullException>(() => Wdc5Format.Register(null!));
    }

    [Fact]
    public void Register_registers_format()
    {
        var registry = new Db2FormatRegistry();
        Wdc5Format.Register(registry);

        registry.Formats.Count.ShouldBe(1);
        registry.Formats[0].ShouldBeOfType<Wdc5Format>();
    }
}
