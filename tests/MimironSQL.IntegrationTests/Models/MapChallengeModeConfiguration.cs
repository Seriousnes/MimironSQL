using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace MimironSQL;

public partial class MapChallengeModeConfiguration
{
    partial void ConfigureNavigation(EntityTypeBuilder<MapChallengeMode> builder)
    {
        builder.HasOne(x => x.Map)
            .WithMany(x => x.MapChallengeModes)
            .HasForeignKey(x => x.MapID);
    }
}
