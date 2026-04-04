namespace MimironSQL.EntityFrameworkCore;

/// <summary>
/// Extensions for enabling and configuring on-disk custom column indexes.
/// </summary>
public static class MimironDb2IndexExtensions
{
    /// <summary>
    /// Enables custom column indexes for the current context options and optionally configures their cache location.
    /// </summary>
    /// <param name="builder">The DB2 options builder.</param>
    /// <param name="configure">Optional callback used to override index options.</param>
    /// <returns>The same builder instance.</returns>
    public static IMimironDb2DbContextOptionsBuilder WithCustomIndexes(
        this IMimironDb2DbContextOptionsBuilder builder,
        Action<Db2IndexOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (builder is not MimironDb2DbContextOptionsBuilder concreteBuilder)
        {
            throw new NotSupportedException("WithCustomIndexes must be called inside UseMimironDb2(...).");
        }

        var options = new Db2IndexOptions();
        configure?.Invoke(options);

        concreteBuilder.SetCustomIndexes(enableCustomIndexes: true, options.CacheDirectory);
        return builder;
    }
}
