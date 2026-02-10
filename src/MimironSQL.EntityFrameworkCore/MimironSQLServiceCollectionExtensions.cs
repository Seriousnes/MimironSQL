using Microsoft.Extensions.DependencyInjection;

namespace MimironSQL.EntityFrameworkCore;

/// <summary>
/// Service registration helpers for composing MimironSQL services outside of a <see cref="Microsoft.EntityFrameworkCore.DbContext"/>.
/// </summary>
public static class MimironSQLServiceCollectionExtensions
{
    /// <summary>
    /// Registers MimironSQL core services needed to query DB2 files.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The same <paramref name="services"/> instance to enable chaining.</returns>
    public static IServiceCollection AddMimironSQLServices(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        MimironDb2ServiceCollectionExtensions.AddCoreServices(services);
        return services;
    }
}
