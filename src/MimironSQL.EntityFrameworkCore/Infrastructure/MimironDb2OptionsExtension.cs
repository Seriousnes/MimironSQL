using System.Text;

using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

using MimironSQL.Dbd;
using MimironSQL.Providers;

namespace MimironSQL.EntityFrameworkCore.Infrastructure;

/// <summary>
/// EF Core options extension used to store provider selection and register MimironSQL DB2 services.
/// </summary>
public class MimironDb2OptionsExtension : IDbContextOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public MimironDb2OptionsExtension()
    {
    }

    protected MimironDb2OptionsExtension(MimironDb2OptionsExtension copyFrom)
    {
        ProviderKey = copyFrom.ProviderKey;
        ProviderConfigHash = copyFrom.ProviderConfigHash;
        ApplyProviderServices = copyFrom.ApplyProviderServices;
    }

    public virtual DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

    /// <summary>
    /// Gets the configured provider key, or <see langword="null"/> if no provider has been selected.
    /// </summary>
    public string? ProviderKey { get; private set; }

    /// <summary>
    /// Gets a hash of the provider configuration used for EF Core service provider caching.
    /// </summary>
    public int ProviderConfigHash { get; private set; }

    /// <summary>
    /// Gets the callback that registers provider-specific services.
    /// </summary>
    public Action<IServiceCollection>? ApplyProviderServices { get; private set; }

    /// <summary>
    /// Returns a copy of this extension configured with the specified provider information.
    /// </summary>
    /// <param name="providerKey">A stable identifier for the provider.</param>
    /// <param name="providerConfigHash">A hash representing the provider configuration.</param>
    /// <param name="applyProviderServices">A callback to register provider-specific services.</param>
    /// <returns>A configured copy of this extension.</returns>
    public MimironDb2OptionsExtension WithProvider(
        string providerKey,
        int providerConfigHash,
        Action<IServiceCollection> applyProviderServices)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerKey);
        ArgumentNullException.ThrowIfNull(applyProviderServices);

        var clone = Clone();
        clone.ProviderKey = providerKey;
        clone.ProviderConfigHash = providerConfigHash;
        clone.ApplyProviderServices = applyProviderServices;
        return clone;
    }

    protected virtual MimironDb2OptionsExtension Clone() => new(this);

    /// <summary>
    /// Registers services required by the provider.
    /// </summary>
    /// <param name="services">The service collection.</param>
    public void ApplyServices(IServiceCollection services)
    {
        services.AddSingleton<IModelCacheKeyFactory, MimironDb2ModelCacheKeyFactory>();
        services.AddSingleton<IModelCustomizer, MimironDb2ModelCustomizer>();

        // Contracts-level defaults that are safe to override.
        services.TryAddSingleton<IDbdParser, DbdParser>();
        services.TryAddSingleton<ITactKeyProvider, NullTactKeyProvider>();

        ApplyProviderServices?.Invoke(services);
        MimironDb2ServiceCollectionExtensions.AddCoreServices(services);
    }

    /// <summary>
    /// Validates that the provider has been configured.
    /// </summary>
    /// <param name="options">The options being validated.</param>
    public void Validate(IDbContextOptions options)
    {
        if (ProviderKey is null || ApplyProviderServices is null)
            throw new InvalidOperationException(
                $"MimironDb2 providers must be configured. Call {nameof(MimironDb2DbContextOptionsExtensions.UseMimironDb2)} to configure the provider.");
    }

    private sealed class NullTactKeyProvider : ITactKeyProvider
    {
        public bool TryGetKey(ulong tactKeyLookup, out ReadOnlyMemory<byte> key)
        {
            key = default;
            return false;
        }
    }

    private sealed class ExtensionInfo(IDbContextOptionsExtension extension) : DbContextOptionsExtensionInfo(extension)
    {
        private string? _logFragment;

        private new MimironDb2OptionsExtension Extension
            => (MimironDb2OptionsExtension)base.Extension;

        public override bool IsDatabaseProvider => true;

        public override string LogFragment
        {
            get
            {
                if (_logFragment == null)
                {
                    var builder = new StringBuilder();
                    builder.Append("MimironDb2");

                    if (Extension.ProviderKey is not null)
                    {
                        builder.Append(":Provider=");
                        builder.Append(Extension.ProviderKey);
                    }

                    _logFragment = builder.ToString();
                }
                return _logFragment;
            }
        }

        public override int GetServiceProviderHashCode()
        {
            var hashCode = new HashCode();
            hashCode.Add(Extension.ProviderKey, StringComparer.Ordinal);
            hashCode.Add(Extension.ProviderConfigHash);
            return hashCode.ToHashCode();
        }

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
            => other is ExtensionInfo otherInfo
                && string.Equals(Extension.ProviderKey, otherInfo.Extension.ProviderKey, StringComparison.Ordinal)
                && Extension.ProviderConfigHash == otherInfo.Extension.ProviderConfigHash;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
            if (Extension.ProviderKey is not null)
                debugInfo["MimironDb2:Provider"] = Extension.ProviderKey;

            debugInfo["MimironDb2:ProviderConfigHash"] = Extension.ProviderConfigHash.ToString();
        }
    }
}
