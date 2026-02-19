using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace MimironSQL.EntityFrameworkCore.Infrastructure;

internal sealed class MimironDb2OptionsExtension : IDbContextOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public string? ProviderKey { get; private init; }
    public int ProviderConfigHash { get; private init; }
    public Action<IServiceCollection>? ApplyProviderServices { get; private init; }
    public string? WowVersion { get; private init; }

    public bool RelaxLayoutValidation { get; private init; }

    public ForeignKeyArrayModeling ForeignKeyArrayModeling { get; private init; } = ForeignKeyArrayModeling.SharedTypeJoinEntity;

    public DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

    public void ApplyServices(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddEntityFrameworkMimironDb2();

        services.AddSingleton<IModelCustomizer, MimironDb2ModelCustomizer>();

        ApplyProviderServices?.Invoke(services);
    }

    public void Validate(IDbContextOptions options)
    {
        // Intentionally minimal for now; this provider is being rebuilt from scratch.
    }

    public MimironDb2OptionsExtension WithWowVersion(string wowVersion)
        => new()
        {
            ProviderKey = ProviderKey,
            ProviderConfigHash = ProviderConfigHash,
            ApplyProviderServices = ApplyProviderServices,
            WowVersion = wowVersion,
            ForeignKeyArrayModeling = ForeignKeyArrayModeling,
            RelaxLayoutValidation = RelaxLayoutValidation,
        };

    public MimironDb2OptionsExtension WithProvider(string providerKey, int providerConfigHash, Action<IServiceCollection> applyProviderServices)
        => new()
        {
            ProviderKey = providerKey,
            ProviderConfigHash = providerConfigHash,
            ApplyProviderServices = applyProviderServices,
            WowVersion = WowVersion,
            ForeignKeyArrayModeling = ForeignKeyArrayModeling,
            RelaxLayoutValidation = RelaxLayoutValidation,
        };

    public MimironDb2OptionsExtension WithForeignKeyArrayModeling(ForeignKeyArrayModeling modeling)
        => new()
        {
            ProviderKey = ProviderKey,
            ProviderConfigHash = ProviderConfigHash,
            ApplyProviderServices = ApplyProviderServices,
            WowVersion = WowVersion,
            ForeignKeyArrayModeling = modeling,
            RelaxLayoutValidation = RelaxLayoutValidation,
        };

    public MimironDb2OptionsExtension WithRelaxLayoutValidation(bool relaxLayoutValidation)
        => new()
        {
            ProviderKey = ProviderKey,
            ProviderConfigHash = ProviderConfigHash,
            ApplyProviderServices = ApplyProviderServices,
            WowVersion = WowVersion,
            ForeignKeyArrayModeling = ForeignKeyArrayModeling,
            RelaxLayoutValidation = relaxLayoutValidation,
        };

    private sealed class ExtensionInfo(IDbContextOptionsExtension extension) : DbContextOptionsExtensionInfo(extension)
    {
        public override bool IsDatabaseProvider => true;

        public override string LogFragment => "using MimironDb2 ";

        public override int GetServiceProviderHashCode()
        {
            var e = (MimironDb2OptionsExtension)Extension;
            return HashCode.Combine(e.ProviderKey, e.ProviderConfigHash, e.WowVersion, (int)e.ForeignKeyArrayModeling, e.RelaxLayoutValidation);
        }

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
            => other is ExtensionInfo;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
            var e = (MimironDb2OptionsExtension)Extension;
            debugInfo["MimironDb2:ProviderKey"] = e.ProviderKey ?? string.Empty;
            debugInfo["MimironDb2:ProviderConfigHash"] = e.ProviderConfigHash.ToString();
            debugInfo["MimironDb2:WowVersion"] = e.WowVersion ?? string.Empty;
            debugInfo["MimironDb2:ForeignKeyArrayModeling"] = e.ForeignKeyArrayModeling.ToString();
            debugInfo["MimironDb2:RelaxLayoutValidation"] = e.RelaxLayoutValidation ? "1" : "0";
        }
    }
}
