using Microsoft.Extensions.DependencyInjection;

namespace MimironSQL.EntityFrameworkCore;

public interface IMimironDb2DbContextOptionsBuilder
{
    IMimironDb2DbContextOptionsBuilder ConfigureProvider(
        string providerKey,
        int providerConfigHash,
        Action<IServiceCollection> applyProviderServices);
}
