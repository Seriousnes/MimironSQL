using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

using MimironSQL.EntityFrameworkCore.Index;
using MimironSQL.Formats.Wdc5;

namespace MimironSQL.EntityFrameworkCore.Infrastructure;

internal sealed class MimironDb2OptionsExtension : IDbContextOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public string? ProviderKey { get; private init; }
    public int ProviderConfigHash { get; private init; }
    public Action<IServiceCollection>? ApplyProviderServices { get; private init; }
    public string? WowVersion { get; private init; }

    public bool RelaxLayoutValidation { get; private init; }

    public bool EagerSparseOffsetTable { get; private init; }

    public bool EnableCustomIndexes { get; private init; }

    public string? CustomIndexCacheDirectory { get; private init; }

    public ForeignKeyArrayModeling ForeignKeyArrayModeling { get; private init; } = ForeignKeyArrayModeling.SharedTypeJoinEntity;

    public DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

    public void ApplyServices(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddEntityFrameworkMimironDb2();

        services.AddSingleton<IModelCustomizer, MimironDb2ModelCustomizer>();
        services.AddSingleton(new Wdc5FormatOptions { EagerSparseOffsetTable = EagerSparseOffsetTable });

        if (EnableCustomIndexes)
        {
            services.AddSingleton(_ => new Db2IndexCacheLocator(CustomIndexCacheDirectory));
            services.AddSingleton(sp => new Db2IndexBuilder(
                sp.GetRequiredService<Db2IndexCacheLocator>(),
                WowVersion ?? throw new InvalidOperationException("MimironDb2 WOW_VERSION is required when custom indexes are enabled.")));
            services.AddSingleton(sp => new Db2IndexLookup(
                sp.GetRequiredService<Db2IndexCacheLocator>(),
                WowVersion ?? throw new InvalidOperationException("MimironDb2 WOW_VERSION is required when custom indexes are enabled.")));
        }

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
            EagerSparseOffsetTable = EagerSparseOffsetTable,
            EnableCustomIndexes = EnableCustomIndexes,
            CustomIndexCacheDirectory = CustomIndexCacheDirectory,
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
            EagerSparseOffsetTable = EagerSparseOffsetTable,
            EnableCustomIndexes = EnableCustomIndexes,
            CustomIndexCacheDirectory = CustomIndexCacheDirectory,
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
            EagerSparseOffsetTable = EagerSparseOffsetTable,
            EnableCustomIndexes = EnableCustomIndexes,
            CustomIndexCacheDirectory = CustomIndexCacheDirectory,
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
            EagerSparseOffsetTable = EagerSparseOffsetTable,
            EnableCustomIndexes = EnableCustomIndexes,
            CustomIndexCacheDirectory = CustomIndexCacheDirectory,
        };

    public MimironDb2OptionsExtension WithEagerSparseOffsetTable(bool eagerSparseOffsetTable)
        => new()
        {
            ProviderKey = ProviderKey,
            ProviderConfigHash = ProviderConfigHash,
            ApplyProviderServices = ApplyProviderServices,
            WowVersion = WowVersion,
            ForeignKeyArrayModeling = ForeignKeyArrayModeling,
            RelaxLayoutValidation = RelaxLayoutValidation,
            EagerSparseOffsetTable = eagerSparseOffsetTable,
            EnableCustomIndexes = EnableCustomIndexes,
            CustomIndexCacheDirectory = CustomIndexCacheDirectory,
        };

    public MimironDb2OptionsExtension WithCustomIndexes(bool enableCustomIndexes, string? customIndexCacheDirectory)
        => new()
        {
            ProviderKey = ProviderKey,
            ProviderConfigHash = ProviderConfigHash,
            ApplyProviderServices = ApplyProviderServices,
            WowVersion = WowVersion,
            ForeignKeyArrayModeling = ForeignKeyArrayModeling,
            RelaxLayoutValidation = RelaxLayoutValidation,
            EagerSparseOffsetTable = EagerSparseOffsetTable,
            EnableCustomIndexes = enableCustomIndexes,
            CustomIndexCacheDirectory = customIndexCacheDirectory,
        };

    private sealed class ExtensionInfo(IDbContextOptionsExtension extension) : DbContextOptionsExtensionInfo(extension)
    {
        public override bool IsDatabaseProvider => true;

        public override string LogFragment => "using MimironDb2 ";

        public override int GetServiceProviderHashCode()
        {
            var e = (MimironDb2OptionsExtension)Extension;
            return HashCode.Combine(
                e.ProviderKey,
                e.ProviderConfigHash,
                e.WowVersion,
                (int)e.ForeignKeyArrayModeling,
                e.RelaxLayoutValidation,
                e.EagerSparseOffsetTable,
                e.EnableCustomIndexes,
                e.CustomIndexCacheDirectory);
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
            debugInfo["MimironDb2:EagerSparseOffsetTable"] = e.EagerSparseOffsetTable ? "1" : "0";
            debugInfo["MimironDb2:EnableCustomIndexes"] = e.EnableCustomIndexes ? "1" : "0";
            debugInfo["MimironDb2:CustomIndexCacheDirectory"] = e.CustomIndexCacheDirectory ?? string.Empty;
        }
    }
}
