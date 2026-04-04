using System.Collections.Immutable;
using System.Text.RegularExpressions;

namespace MimironSQL.DbContextGenerator.Filters;

internal readonly struct FilterSet(BlockFilter block, AllowFilter allow)
{
    public BlockFilter Block { get; } = block;

    public AllowFilter Allow { get; } = allow;

    public bool IsAllowed(string dbdFileNameWithoutExtension)
    {
        if (Allow.IsMatch(dbdFileNameWithoutExtension))
        {
            return true;
        }

        if (Block.IsMatch(dbdFileNameWithoutExtension))
        {
            return false;
        }

        return true;
    }

    public static FilterSet Empty { get; } = new(BlockFilter.Empty, AllowFilter.Empty);

    public static FilterSet Create(ImmutableArray<string> lines)
    {
        if (lines is { Length: 0 })
        {
            return Empty;
        }

        List<Regex> block = [];
        List<Regex> allow = [];

        foreach (var line in lines)
        {
            if (line is not { Length: > 0 })
            {
                continue;
            }

            var op = line[0];
            if (op is not ('!' or '~'))
            {
                continue;
            }

            var pattern = line.Substring(1).Trim();
            if (pattern.Length == 0)
            {
                continue;
            }

            if (!TryCreateRegex(pattern, out var regex))
            {
                continue;
            }

            if (op == '!')
            {
                block.Add(regex);
            }
            else
            {
                allow.Add(regex);
            }
        }

        var blockFilter = block.Count == 0 ? BlockFilter.Empty : new BlockFilter([.. block]);
        var allowFilter = allow.Count == 0 ? AllowFilter.Empty : new AllowFilter([.. allow]);
        return new FilterSet(blockFilter, allowFilter);
    }

    private static bool TryCreateRegex(string pattern, out Regex regex)
    {
        try
        {
            regex = new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
            return true;
        }
        catch (ArgumentException)
        {
            regex = null!;
            return false;
        }
    }
}