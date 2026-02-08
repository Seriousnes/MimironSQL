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
    public void Register_registers_instance()
    {
        var registry = new Db2FormatRegistry();
        Wdc5Format.Register(registry);

        registry.Formats.ShouldContain(Wdc5Format.Instance);
    }
}
