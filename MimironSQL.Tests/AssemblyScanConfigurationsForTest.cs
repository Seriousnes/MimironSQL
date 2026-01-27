using MimironSQL.Db2.Model;
using MimironSQL.Tests.Fixtures;

namespace MimironSQL.Tests.AssemblyScanConfigurations;

internal sealed class MapForScanConfiguration : IDb2EntityTypeConfiguration<Map>
{
    public void Configure(Db2EntityTypeBuilder<Map> builder)
        => builder
            .ToTable("Map")
            .HasOne(m => m.ParentMap)
            .WithForeignKey(m => m.ParentMapID);
}

internal sealed class SpellForScanConfiguration : IDb2EntityTypeConfiguration<Spell>
{
    public void Configure(Db2EntityTypeBuilder<Spell> builder)
        => builder.ToTable("Spell");
}
