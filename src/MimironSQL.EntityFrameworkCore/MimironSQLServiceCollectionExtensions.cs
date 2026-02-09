using Microsoft.Extensions.DependencyInjection;

namespace MimironSQL.EntityFrameworkCore;

public static class MimironSQLServiceCollectionExtensions
{
    public static IServiceCollection AddMimironSQLServices(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        MimironDb2ServiceCollectionExtensions.AddCoreServices(services);
        return services;
    }
}
