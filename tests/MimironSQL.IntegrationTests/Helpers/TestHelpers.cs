using Microsoft.EntityFrameworkCore;

using MimironSQL.EntityFrameworkCore;

namespace MimironSQL.IntegrationTests.Helpers;

internal static class TestHelpers
{
    public const string WowVersion = "12.0.1.65867";

    public static DbContextOptionsBuilder<TContext> UseMimironDb2ForTests<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        Action<IMimironDb2DbContextOptionsBuilder> configure)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentNullException.ThrowIfNull(configure);        

        return optionsBuilder;
    }
}
