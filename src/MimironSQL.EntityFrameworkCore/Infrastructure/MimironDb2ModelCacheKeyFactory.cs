using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

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
            extension.ProviderType,
            extension.Db2Path,
            extension.DbdDefinitionsPath,
            designTime);
    }

    public object Create(DbContext context)
        => Create(context, designTime: false);
}
