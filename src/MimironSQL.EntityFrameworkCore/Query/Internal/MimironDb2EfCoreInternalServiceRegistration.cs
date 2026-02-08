using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

internal static class MimironDb2EfCoreInternalServiceRegistration
{
#pragma warning disable EF1001 // Internal EF Core API usage is isolated to this registration.
    public static void Add(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.Replace(ServiceDescriptor.Scoped<IQueryCompiler, MimironDb2EfCoreQueryCompiler>());
    }
#pragma warning restore EF1001
}
