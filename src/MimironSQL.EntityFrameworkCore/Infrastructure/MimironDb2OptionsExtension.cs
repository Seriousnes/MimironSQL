using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace MimironSQL.EntityFrameworkCore;

public class MimironDb2OptionsExtension : IDbContextOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public MimironDb2OptionsExtension()
    {
    }

    protected MimironDb2OptionsExtension(MimironDb2OptionsExtension copyFrom)
    {
        ProviderType = copyFrom.ProviderType;
        Db2Path = copyFrom.Db2Path;
        DbdDefinitionsPath = copyFrom.DbdDefinitionsPath;
    }

    public virtual DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

    public MimironDb2ProviderType ProviderType { get; private set; }
    public string? Db2Path { get; private set; }
    public string? DbdDefinitionsPath { get; private set; }

    public MimironDb2OptionsExtension WithFileSystem(string db2DirectoryPath, string? dbdDefinitionsPath)
    {
        var clone = Clone();
        clone.ProviderType = MimironDb2ProviderType.FileSystem;
        clone.Db2Path = db2DirectoryPath;
        clone.DbdDefinitionsPath = dbdDefinitionsPath;
        return clone;
    }

    public MimironDb2OptionsExtension WithCasc(string cascRootPath, string? dbdDefinitionsPath)
    {
        var clone = Clone();
        clone.ProviderType = MimironDb2ProviderType.Casc;
        clone.Db2Path = cascRootPath;
        clone.DbdDefinitionsPath = dbdDefinitionsPath;
        return clone;
    }

    protected virtual MimironDb2OptionsExtension Clone() => new(this);

    public void ApplyServices(IServiceCollection services)
    {
    }

    public void Validate(IDbContextOptions options)
    {
        if (string.IsNullOrWhiteSpace(Db2Path))
        {
            throw new InvalidOperationException(
                $"DB2 path must be configured. Call {nameof(MimironDb2DbContextOptionsExtensions.UseMimironDb2FileSystem)} or {nameof(MimironDb2DbContextOptionsExtensions.UseMimironDb2Casc)} to configure the provider.");
        }
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
                    builder.Append("MimironDb2:");
                    builder.Append(Extension.ProviderType);
                    if (!string.IsNullOrWhiteSpace(Extension.Db2Path))
                    {
                        builder.Append('=');
                        builder.Append(Extension.Db2Path);
                    }
                    _logFragment = builder.ToString();
                }
                return _logFragment;
            }
        }

        public override int GetServiceProviderHashCode()
        {
            var hashCode = new HashCode();
            hashCode.Add(Extension.ProviderType);
            hashCode.Add(Extension.Db2Path);
            hashCode.Add(Extension.DbdDefinitionsPath);
            return hashCode.ToHashCode();
        }

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
            => other is ExtensionInfo otherInfo
                && Extension.ProviderType == otherInfo.Extension.ProviderType
                && Extension.Db2Path == otherInfo.Extension.Db2Path
                && Extension.DbdDefinitionsPath == otherInfo.Extension.DbdDefinitionsPath;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
            debugInfo["MimironDb2:ProviderType"] = Extension.ProviderType.ToString();
            if (!string.IsNullOrWhiteSpace(Extension.Db2Path))
            {
                debugInfo["MimironDb2:Path"] = Extension.Db2Path;
            }
            if (!string.IsNullOrWhiteSpace(Extension.DbdDefinitionsPath))
            {
                debugInfo["MimironDb2:DbdDefinitionsPath"] = Extension.DbdDefinitionsPath;
            }
        }
    }
}

public enum MimironDb2ProviderType
{
    FileSystem,
    Casc
}
