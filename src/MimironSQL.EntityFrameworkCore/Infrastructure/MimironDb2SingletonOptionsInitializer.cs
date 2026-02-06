using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Internal;

namespace MimironSQL.EntityFrameworkCore.Infrastructure;

internal sealed class MimironDb2SingletonOptionsInitializer : ISingletonOptionsInitializer
{
    public void EnsureInitialized(IServiceProvider serviceProvider, IDbContextOptions options)
    {
        // No singleton options to initialize for this provider
    }
}
