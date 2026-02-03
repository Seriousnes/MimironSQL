using MimironSQL.Db2.Model;

namespace MimironSQL.Tests.AssemblyScanConfigurations;

internal sealed class MapForScanConfiguration : IDb2EntityTypeConfiguration<Fixtures.Map>
{
    public void Configure(Db2EntityTypeBuilder<Fixtures.Map> builder)
        => builder
            .ToTable("Map")
            .HasOne(m => m.ParentMap)
            .WithForeignKey(m => m.ParentMapID);
}

internal sealed class SpellForScanConfiguration : IDb2EntityTypeConfiguration<Fixtures.Spell>
{
    public void Configure(Db2EntityTypeBuilder<Fixtures.Spell> builder)
        => builder.ToTable("Spell");
}
