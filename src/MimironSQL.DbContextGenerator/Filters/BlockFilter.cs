using System.Collections.Immutable;
using System.Text.RegularExpressions;

namespace MimironSQL.DbContextGenerator.Filters;

internal sealed class BlockFilter(ImmutableArray<Regex> expressions) : Filter(expressions)
{
    public static BlockFilter Empty { get; } = new([]);
}
