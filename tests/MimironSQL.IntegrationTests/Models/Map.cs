using System.ComponentModel.DataAnnotations.Schema;

namespace MimironSQL;

public partial class Map
{
    public Map? ParentMap { get; set; }    
    public ICollection<MapChallengeMode> MapChallengeModes { get; set; } = null!;
}