namespace MimironSQL.EntityFrameworkCore;

/// <summary>
/// Provider-level options for FK-array navigations.
/// </summary>
public static class MimironDb2ForeignKeyArrayModelingExtensions
{
    /// <summary>
    /// Controls how FK-array navigations are represented in the EF Core model.
    /// </summary>
    public static IMimironDb2DbContextOptionsBuilder WithForeignKeyArrayModeling(
        this IMimironDb2DbContextOptionsBuilder builder,
        ForeignKeyArrayModeling modeling)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (builder is not MimironDb2DbContextOptionsBuilder b)
        {
            throw new NotSupportedException("WithForeignKeyArrayModeling must be called inside UseMimironDb2(...).");
        }

        b.SetForeignKeyArrayModeling(modeling);
        return builder;
    }
}
