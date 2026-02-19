using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace MimironSQL;

public class TactKeyLookupEntityConfiguration : IEntityTypeConfiguration<TactKeyLookupEntity>
{
    public void Configure(EntityTypeBuilder<TactKeyLookupEntity> builder)
    {
        builder.ToTable("TactKeyLookup");
        builder.HasKey(x => x.Id);
    }
}
