using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace MimironSQL.EntityFrameworkCore.Infrastructure;

/// <summary>
/// Creates model cache keys for MimironDB2 so EF Core can reuse models per provider configuration.
/// </summary>
public class MimironDb2ModelCacheKeyFactory : IModelCacheKeyFactory
{
    /// <inheritdoc />
    public object Create(DbContext context, bool designTime)
    {
        var extension = context.GetService<IDbContextOptions>()
            .FindExtension<MimironDb2OptionsExtension>();

        if (extension is null)
            return (context.GetType(), designTime);

        return (
            context.GetType(),
            extension.ProviderKey ?? string.Empty,
            extension.ProviderConfigHash,
            designTime);
    }

        /// <summary>
        /// Creates a model cache key for the given context.
        /// </summary>
        /// <param name="context">The EF Core context.</param>
        /// <returns>A cache key object.</returns>
    public object Create(DbContext context)
        => Create(context, designTime: false);
}
