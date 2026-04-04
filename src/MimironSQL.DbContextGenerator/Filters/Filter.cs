using System.Collections.Immutable;
using System.Text.RegularExpressions;

namespace MimironSQL.DbContextGenerator.Filters;

internal abstract class Filter(ImmutableArray<Regex> expressions)
{
    public ImmutableArray<Regex> Expressions { get; } = expressions;

    public bool IsMatch(string dbdFileNameWithoutExtension)
    {
        foreach (var r in Expressions)
        {
            if (r.IsMatch(dbdFileNameWithoutExtension))
            {
                return true;
            }
        }

        return false;
    }
}
