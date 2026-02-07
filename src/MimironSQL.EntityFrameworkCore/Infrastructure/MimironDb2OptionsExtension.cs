using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using MimironSQL.Providers;

namespace MimironSQL.EntityFrameworkCore;

public class MimironDb2OptionsExtension : IDbContextOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public MimironDb2OptionsExtension()
    {
    }

    protected MimironDb2OptionsExtension(MimironDb2OptionsExtension copyFrom)
    {
        Db2StreamProvider = copyFrom.Db2StreamProvider;
        DbdProvider = copyFrom.DbdProvider;
        TactKeyProvider = copyFrom.TactKeyProvider;
    }

    public virtual DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

    public IDb2StreamProvider? Db2StreamProvider { get; private set; }
    public IDbdProvider? DbdProvider { get; private set; }
    public ITactKeyProvider? TactKeyProvider { get; private set; }

    public MimironDb2OptionsExtension WithProviders(
        IDb2StreamProvider db2StreamProvider,
        IDbdProvider dbdProvider,
        ITactKeyProvider tactKeyProvider)
    {
        ArgumentNullException.ThrowIfNull(db2StreamProvider);
        ArgumentNullException.ThrowIfNull(dbdProvider);
        ArgumentNullException.ThrowIfNull(tactKeyProvider);

        var clone = Clone();
        clone.Db2StreamProvider = db2StreamProvider;
        clone.DbdProvider = dbdProvider;
        clone.TactKeyProvider = tactKeyProvider;
        return clone;
    }

    protected virtual MimironDb2OptionsExtension Clone() => new(this);

    public void ApplyServices(IServiceCollection services)
    {
        services.AddSingleton<IModelCacheKeyFactory, MimironDb2ModelCacheKeyFactory>();
        services.AddSingleton<IModelCustomizer, MimironDb2ModelCustomizer>();
        if (Db2StreamProvider is not null)
            services.AddSingleton(Db2StreamProvider);

        if (DbdProvider is not null)
            services.AddSingleton(DbdProvider);

        if (TactKeyProvider is not null)
            services.AddSingleton(TactKeyProvider);

        MimironDb2ServiceCollectionExtensions.AddCoreServices(services);
    }

    public void Validate(IDbContextOptions options)
    {
        if (Db2StreamProvider is null || DbdProvider is null || TactKeyProvider is null)
            throw new InvalidOperationException(
                $"MimironDb2 providers must be configured. Call {nameof(MimironDb2DbContextOptionsExtensions.UseMimironDb2)} to configure the provider.");
    }

    private sealed class ExtensionInfo : DbContextOptionsExtensionInfo
    {
        private string? _logFragment;

        public ExtensionInfo(IDbContextOptionsExtension extension)
            : base(extension)
        {
        }

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

                    if (Extension.Db2StreamProvider is not null)
                    {
                        builder.Append(":Db2=");
                        builder.Append(Extension.Db2StreamProvider.GetType().Name);
                    }

                    if (Extension.DbdProvider is not null)
                    {
                        builder.Append(":Dbd=");
                        builder.Append(Extension.DbdProvider.GetType().Name);
                    }

                    if (Extension.TactKeyProvider is not null)
                    {
                        builder.Append(":TactKeys=");
                        builder.Append(Extension.TactKeyProvider.GetType().Name);
                    }

                    _logFragment = builder.ToString();
                }
                return _logFragment;
            }
        }

        public override int GetServiceProviderHashCode()
        {
            var hashCode = new HashCode();
            hashCode.Add(Extension.Db2StreamProvider is null ? 0 : RuntimeHelpers.GetHashCode(Extension.Db2StreamProvider));
            hashCode.Add(Extension.DbdProvider is null ? 0 : RuntimeHelpers.GetHashCode(Extension.DbdProvider));
            hashCode.Add(Extension.TactKeyProvider is null ? 0 : RuntimeHelpers.GetHashCode(Extension.TactKeyProvider));
            return hashCode.ToHashCode();
        }

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
            => other is ExtensionInfo otherInfo
                && ReferenceEquals(Extension.Db2StreamProvider, otherInfo.Extension.Db2StreamProvider)
                && ReferenceEquals(Extension.DbdProvider, otherInfo.Extension.DbdProvider)
                && ReferenceEquals(Extension.TactKeyProvider, otherInfo.Extension.TactKeyProvider);

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
            if (Extension.Db2StreamProvider is not null)
                debugInfo["MimironDb2:Db2StreamProvider"] = Extension.Db2StreamProvider.GetType().FullName ?? Extension.Db2StreamProvider.GetType().Name;

            if (Extension.DbdProvider is not null)
                debugInfo["MimironDb2:DbdProvider"] = Extension.DbdProvider.GetType().FullName ?? Extension.DbdProvider.GetType().Name;

            if (Extension.TactKeyProvider is not null)
                debugInfo["MimironDb2:TactKeyProvider"] = Extension.TactKeyProvider.GetType().FullName ?? Extension.TactKeyProvider.GetType().Name;
        }
    }
}
