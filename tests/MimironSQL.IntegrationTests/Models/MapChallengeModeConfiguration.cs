using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace MimironSQL;

public partial class MapChallengeModeEntityConfiguration
{
    partial void ConfigureNavigation(EntityTypeBuilder<MapChallengeModeEntity> builder)
    {
        builder.HasOne(x => x.Map)
            .WithMany(x => x.MapChallengeModes)
            .HasForeignKey(x => x.MapID);
    }
}
