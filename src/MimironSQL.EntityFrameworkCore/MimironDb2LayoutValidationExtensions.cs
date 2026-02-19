namespace MimironSQL.EntityFrameworkCore;

/// <summary>
/// Extensions for configuring DB2 layout validation behavior.
/// </summary>
public static class MimironDb2LayoutValidationExtensions
{
    /// <summary>
    /// Disables strict DB2 layout validation (layout hash and physical field count) for the current context.
    /// Intended for tests that use fake/in-memory DB2 formats.
    /// </summary>
    /// <param name="builder">The options builder.</param>
    /// <param name="relaxed">Whether validation should be relaxed.</param>
    /// <returns>The same builder instance to enable chaining.</returns>
    public static IMimironDb2DbContextOptionsBuilder WithRelaxedLayoutValidation(
        this IMimironDb2DbContextOptionsBuilder builder,
        bool relaxed = true)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (builder is not MimironDb2DbContextOptionsBuilder b)
        {
            throw new InvalidOperationException(
                $"The {nameof(IMimironDb2DbContextOptionsBuilder)} instance must be a {nameof(MimironDb2DbContextOptionsBuilder)}.");
        }

        b.SetRelaxLayoutValidation(relaxed);
        return builder;
    }
}
