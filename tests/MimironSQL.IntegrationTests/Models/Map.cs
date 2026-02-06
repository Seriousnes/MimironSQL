using System.ComponentModel.DataAnnotations.Schema;

namespace MimironSQL;

public partial class Map
{
    [ForeignKey(nameof(MapChallengeMode.MapID))]
    public ICollection<MapChallengeMode> MapChallengeModes { get; set; } = [];
}