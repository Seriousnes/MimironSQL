using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using System.Runtime.CompilerServices;

namespace MimironSQL.EntityFrameworkCore;

public class MimironDb2ModelCacheKeyFactory : IModelCacheKeyFactory
{
    public object Create(DbContext context, bool designTime)
    {
        var extension = context.GetService<IDbContextOptions>()
            .FindExtension<MimironDb2OptionsExtension>();

        if (extension is null)
            return (context.GetType(), designTime);

        return (
            context.GetType(),
            extension.Db2StreamProvider is null ? 0 : RuntimeHelpers.GetHashCode(extension.Db2StreamProvider),
            extension.DbdProvider is null ? 0 : RuntimeHelpers.GetHashCode(extension.DbdProvider),
            extension.TactKeyProvider is null ? 0 : RuntimeHelpers.GetHashCode(extension.TactKeyProvider),
            designTime);
    }

    public object Create(DbContext context)
        => Create(context, designTime: false);
}
