using MimironSQL.Formats.Wdc5;

namespace MimironSQL.EntityFrameworkCore;

/// <summary>
/// Provider-level options for WDC5-specific behavior.
/// </summary>
public static class MimironDb2Wdc5Extensions
{
    /// <summary>
    /// Configures WDC5-specific options used by the format reader.
    /// </summary>
    public static IMimironDb2DbContextOptionsBuilder ConfigureWdc5(
        this IMimironDb2DbContextOptionsBuilder builder,
        Action<Wdc5FormatOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        if (builder is not MimironDb2DbContextOptionsBuilder concreteBuilder)
            throw new NotSupportedException("ConfigureWdc5 must be called inside UseMimironDb2(...).");

        var options = new Wdc5FormatOptions();
        configure(options);
        concreteBuilder.SetEagerSparseOffsetTable(options.EagerSparseOffsetTable);
        return builder;
    }
}