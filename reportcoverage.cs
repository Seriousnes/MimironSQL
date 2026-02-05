using System.Globalization;
using System.Xml.Linq;

static string? TryGetArg(string[] args, string name)
{
    for (var i = 0; i < args.Length - 1; i++)
    {
        if (string.Equals(args[i], name, StringComparison.OrdinalIgnoreCase))
            return args[i + 1];
    }

    return null;
}

static bool HasFlag(string[] args, string name)
    => args.Any(a => string.Equals(a, name, StringComparison.OrdinalIgnoreCase));

static string? FindLatestCobertura(string repoRoot)
{
    var history = Path.Combine(repoRoot, "coverage", "history");
    if (!Directory.Exists(history))
        return null;

    var candidates = Directory.EnumerateDirectories(history)
        .Select(d => new
        {
            Dir = d,
            Name = Path.GetFileName(d),
            Cobertura = Path.Combine(d, "merged", "Cobertura.xml"),
        })
        .Where(x => File.Exists(x.Cobertura))
        .OrderByDescending(x => x.Name, StringComparer.Ordinal)
        .ToArray();

    return candidates.FirstOrDefault()?.Cobertura;
}

static int ParseInt(string? value)
    => int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : 0;

static void PrintUsage()
{
    Console.WriteLine("Usage:");
    Console.WriteLine("  dotnet run reportcoverage.cs -- [--cobertura <path>] [--filter <substring>] [--top <N>] [--files] [--percent]");
    Console.WriteLine("  dotnet run reportcoverage.cs -- [--cobertura <path>] --class <substring> [--methods] [--missed-lines] [--top <N>]");
    Console.WriteLine();
    Console.WriteLine("Defaults:");
    Console.WriteLine("  --cobertura: latest coverage/history/*/merged/Cobertura.xml");
    Console.WriteLine("  --top: 30");
    Console.WriteLine();
    Console.WriteLine("Class drill-down:");
    Console.WriteLine("  --class <substring>: select a single Cobertura <class> by name substring (case-insensitive)");
    Console.WriteLine("  --methods:           print per-method missed line ranges for the selected class");
    Console.WriteLine("  --missed-lines:      print missed line ranges for the selected class (from class-level <lines>)");
    Console.WriteLine();
    Console.WriteLine("Sorting:");
    Console.WriteLine("  default: by missed lines (descending)");
    Console.WriteLine("  --percent: by coverage percentage (ascending)");
}

static string FormatPercent(double ratio)
    => $"{ratio * 100.0,6:0.0}%";

static string CompressRanges(IEnumerable<int> numbers)
{
    var ordered = numbers
        .Distinct()
        .OrderBy(static x => x)
        .ToArray();

    if (ordered.Length == 0)
        return string.Empty;

    var parts = new List<string>(capacity: 16);
    var start = ordered[0];
    var prev = ordered[0];

    for (var i = 1; i < ordered.Length; i++)
    {
        var n = ordered[i];
        if (n == prev + 1)
        {
            prev = n;
            continue;
        }

        parts.Add(start == prev ? start.ToString(CultureInfo.InvariantCulture) : $"{start}-{prev}");
        start = prev = n;
    }

    parts.Add(start == prev ? start.ToString(CultureInfo.InvariantCulture) : $"{start}-{prev}");
    return string.Join(", ", parts);
}

static string GetSimpleClassName(string fullName)
{
    if (string.IsNullOrWhiteSpace(fullName))
        return string.Empty;

    var dot = fullName.LastIndexOf('.');
    var slash = fullName.LastIndexOf('/');
    var i = Math.Max(dot, slash);
    return i < 0 ? fullName : fullName[(i + 1)..];
}

var cliArgs = Environment.GetCommandLineArgs().Skip(1).ToArray();
if (HasFlag(cliArgs, "--help") || HasFlag(cliArgs, "-h"))
{
    PrintUsage();
    return;
}

var repoRoot = Directory.GetCurrentDirectory();
var coberturaPath = TryGetArg(cliArgs, "--cobertura") ?? FindLatestCobertura(repoRoot);
if (coberturaPath is null)
{
    Console.Error.WriteLine("No Cobertura.xml found. Pass --cobertura <path> or run coverage first.");
    Environment.ExitCode = 1;
    return;
}

var filter = TryGetArg(cliArgs, "--filter");
var classFilter = TryGetArg(cliArgs, "--class");
var topText = TryGetArg(cliArgs, "--top");
var top = int.TryParse(topText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedTop) ? parsedTop : 30;
if (top <= 0)
    top = 30;

var showFiles = HasFlag(cliArgs, "--files");
var sortByPercent = HasFlag(cliArgs, "--percent");
var showMethods = HasFlag(cliArgs, "--methods");
var showMissedLines = HasFlag(cliArgs, "--missed-lines");

Console.WriteLine($"Cobertura: {coberturaPath}");
if (filter is not null)
    Console.WriteLine($"Filter:    '{filter}'");
if (classFilter is not null)
    Console.WriteLine($"Class:     '{classFilter}'");
Console.WriteLine();

var doc = XDocument.Load(coberturaPath);

static int ParseLineNumber(XElement line)
    => ParseInt((string?)line.Attribute("number"));

if (classFilter is not null)
{
    var classes = doc
        .Descendants("class")
        .Select(c => new
        {
            Element = c,
            Name = (string?)c.Attribute("name") ?? string.Empty,
            File = (string?)c.Attribute("filename") ?? string.Empty,
            Lines = c.Element("lines")?.Elements("line") ?? Enumerable.Empty<XElement>(),
        })
        .ToArray();

    var matches = classes
        .Where(c => c.Name.Contains(classFilter, StringComparison.OrdinalIgnoreCase))
        .Select(c =>
        {
            var total = 0;
            var covered = 0;
            var missedLineNumbers = new List<int>(capacity: 128);

            foreach (var line in c.Lines)
            {
                if (line.Attribute("hits") is not { } hitsAttr)
                    continue;

                total++;
                var hits = ParseInt(hitsAttr.Value);
                if (hits > 0)
                {
                    covered++;
                }
                else
                {
                    missedLineNumbers.Add(ParseLineNumber(line));
                }
            }

            return new
            {
                c.Element,
                c.Name,
                c.File,
                Total = total,
                Covered = covered,
                Missed = total - covered,
                Coverage = total == 0 ? 0.0 : (double)covered / total,
                MissedLineNumbers = missedLineNumbers,
            };
        })
        .OrderByDescending(x => x.Missed)
        .ThenBy(x => x.Name, StringComparer.Ordinal)
        .ThenBy(x => x.File, StringComparer.Ordinal)
        .ToArray();

    if (matches.Length == 0)
    {
        Console.Error.WriteLine($"No Cobertura class matched --class '{classFilter}'.");
        Environment.ExitCode = 1;
        return;
    }

    if (matches.Length != 1)
    {
        var simpleNameMatches = matches
            .Where(m => string.Equals(GetSimpleClassName(m.Name), classFilter, StringComparison.OrdinalIgnoreCase))
            .ToArray();

        if (simpleNameMatches.Length == 1)
        {
            matches = simpleNameMatches;
        }
        else
        {
            Console.Error.WriteLine($"--class '{classFilter}' matched multiple classes. Refine the substring.");
            Console.Error.WriteLine();
            Console.Error.WriteLine($"Top {Math.Min(top, matches.Length)} matches by missed lines:");
            foreach (var m in matches.Take(top))
                Console.Error.WriteLine($"{m.Missed,5} missed / {m.Total,5} lines ({FormatPercent(m.Coverage)}) | {m.Name} | {m.File}");

            Environment.ExitCode = 2;
            return;
        }
    }

    var selected = matches[0];
    Console.WriteLine($"Selected class: {selected.Name} | {selected.File}");
    Console.WriteLine($"Lines: {selected.Covered}/{selected.Total} covered ({FormatPercent(selected.Coverage)}), {selected.Missed} missed");

    if (showMissedLines)
    {
        Console.WriteLine();
        Console.WriteLine("Missed line ranges (class-level):");
        Console.WriteLine(CompressRanges(selected.MissedLineNumbers));
    }

    if (showMethods)
    {
        var methods = selected.Element
            .Element("methods")
            ?.Elements("method")
            .Select(m => new
            {
                Name = (string?)m.Attribute("name") ?? string.Empty,
                Signature = (string?)m.Attribute("signature") ?? string.Empty,
                Lines = m.Element("lines")?.Elements("line") ?? Enumerable.Empty<XElement>(),
            })
            .Select(m =>
            {
                var total = 0;
                var covered = 0;
                var missed = new List<int>(capacity: 32);

                foreach (var line in m.Lines)
                {
                    if (line.Attribute("hits") is not { } hitsAttr)
                        continue;

                    total++;
                    var hits = ParseInt(hitsAttr.Value);
                    if (hits > 0)
                        covered++;
                    else
                        missed.Add(ParseLineNumber(line));
                }

                return new
                {
                    m.Name,
                    m.Signature,
                    Total = total,
                    Covered = covered,
                    Missed = total - covered,
                    Coverage = total == 0 ? 0.0 : (double)covered / total,
                    MissedRanges = CompressRanges(missed),
                };
            })
            .Where(m => m.Missed > 0)
            .OrderByDescending(m => m.Missed)
            .ThenBy(m => m.Coverage)
            .ThenBy(m => m.Name, StringComparer.Ordinal)
            .ToArray() ?? [];

        Console.WriteLine();
        Console.WriteLine($"Top {Math.Min(top, methods.Length)} methods by missed lines:");
        foreach (var m in methods.Take(top))
        {
            var sig = string.IsNullOrWhiteSpace(m.Signature) ? string.Empty : $" {m.Signature}";
            Console.WriteLine($"{m.Missed,5} missed / {m.Total,5} lines ({FormatPercent(m.Coverage)}) | {m.Name}{sig} | {m.MissedRanges}");
        }
    }

    return;
}

var classStats = doc
    .Descendants("class")
    .Select(c => new
    {
        Name = (string?)c.Attribute("name") ?? string.Empty,
        File = (string?)c.Attribute("filename") ?? string.Empty,
        Lines = c.Element("lines")?.Elements("line") ?? Enumerable.Empty<XElement>(),
    })
    .Select(x =>
    {
        var total = 0;
        var covered = 0;

        foreach (var line in x.Lines)
        {
            if (line.Attribute("hits") is not { } hitsAttr)
                continue;

            total++;
            if (ParseInt(hitsAttr.Value) > 0)
                covered++;
        }

        return new
        {
            x.Name,
            x.File,
            Total = total,
            Covered = covered,
            Missed = total - covered,
            Coverage = total == 0 ? 0.0 : (double)covered / total,
        };
    })
    .Where(x => x.Total != 0)
    .Where(x => filter is null || x.Name.Contains(filter, StringComparison.OrdinalIgnoreCase) || x.File.Contains(filter, StringComparison.OrdinalIgnoreCase))
    .OrderBy(x => sortByPercent ? x.Coverage : -x.Missed)
    .ThenBy(x => x.Name, StringComparer.Ordinal)
    .ThenBy(x => x.File, StringComparer.Ordinal)
    .ToArray();

Console.WriteLine($"Top {Math.Min(top, classStats.Length)} classes by {(sortByPercent ? "coverage %" : "missed lines")}:" );
foreach (var s in classStats.Take(top))
{
    Console.WriteLine($"{s.Missed,5} missed / {s.Total,5} lines ({s.Coverage * 100.0,6:0.0}%) | {s.Name} | {s.File}");
}

if (showFiles)
{
    Console.WriteLine();

    var fileStats = classStats
        .GroupBy(x => x.File, StringComparer.Ordinal)
        .Select(g => new
        {
            File = g.Key,
            Total = g.Sum(x => x.Total),
            Covered = g.Sum(x => x.Covered),
            Missed = g.Sum(x => x.Missed),
        })
        .Where(x => x.Total != 0)
        .OrderBy(x => sortByPercent ? ((double)x.Covered / x.Total) : -(double)x.Missed)
        .ThenBy(x => x.File, StringComparer.Ordinal)
        .ToArray();

    Console.WriteLine($"Top {Math.Min(top, fileStats.Length)} files by {(sortByPercent ? "coverage %" : "missed lines")}:" );
    foreach (var s in fileStats.Take(top))
    {
        var pct = (double)s.Covered / s.Total * 100.0;
        Console.WriteLine($"{s.Missed,5} missed / {s.Total,5} lines ({pct,6:0.0}%) | {s.File}");
    }
}
