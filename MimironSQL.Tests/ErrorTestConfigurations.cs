using MimironSQL.Db2.Model;

namespace MimironSQL.Tests.ErrorTestConfigurations;

internal sealed class ConfigWithoutParameterlessConstructor : IDb2EntityTypeConfiguration<Map>
{
    public ConfigWithoutParameterlessConstructor(string parameter)
    {
    }

    public void Configure(Db2EntityTypeBuilder<Map> builder)
        => builder.ToTable("Map");
}

internal sealed class ConfigurationThatThrows : IDb2EntityTypeConfiguration<Map>
{
    public ConfigurationThatThrows()
    {
        throw new InvalidOperationException("Test exception from constructor");
    }

    public void Configure(Db2EntityTypeBuilder<Map> builder)
        => builder.ToTable("Map");
}
