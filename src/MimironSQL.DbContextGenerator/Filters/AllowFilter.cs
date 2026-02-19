using System.Collections.Immutable;
using System.Text.RegularExpressions;

namespace MimironSQL.DbContextGenerator.Filters;

internal sealed class AllowFilter(ImmutableArray<Regex> expressions) : Filter(expressions)
{
    public static AllowFilter Empty { get; } = new([]);
}
