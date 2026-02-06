using System.Collections.Generic;

using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace MimironSQL.EntityFrameworkCore.Infrastructure;

internal sealed class MimironDb2OptionsExtension : IDbContextOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

    public void ApplyServices(IServiceCollection services)
    {
        services.AddScoped<MimironDb2ResolvedServices>(static sp => MimironDb2ResolvedServices.Resolve(sp));
    }

    public void Validate(IDbContextOptions options)
    {
    }

    private sealed class ExtensionInfo(IDbContextOptionsExtension extension) : DbContextOptionsExtensionInfo(extension)
    {
        public override bool IsDatabaseProvider => false;

        public override string LogFragment => "using MimironDb2 ";

        public override int GetServiceProviderHashCode() => 0;

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other) => other is ExtensionInfo;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
            => debugInfo["MimironDb2"] = "1";
    }
}
