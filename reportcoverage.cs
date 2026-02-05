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
    Console.WriteLine("  dotnet run reportcoverage.cs -- [--cobertura <path>] [--filter <substring>] [--top <N>] [--files]");
    Console.WriteLine();
    Console.WriteLine("Defaults:");
    Console.WriteLine("  --cobertura: latest coverage/history/*/merged/Cobertura.xml");
    Console.WriteLine("  --top: 30");
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
var topText = TryGetArg(cliArgs, "--top");
var top = int.TryParse(topText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedTop) ? parsedTop : 30;
if (top <= 0)
    top = 30;

var showFiles = HasFlag(cliArgs, "--files");

Console.WriteLine($"Cobertura: {coberturaPath}");
if (filter is not null)
    Console.WriteLine($"Filter:    '{filter}'");
Console.WriteLine();

var doc = XDocument.Load(coberturaPath);

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
    .OrderByDescending(x => x.Missed)
    .ThenBy(x => x.Coverage)
    .ThenBy(x => x.Name, StringComparer.Ordinal)
    .ToArray();

Console.WriteLine($"Top {Math.Min(top, classStats.Length)} classes by missed lines:");
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
        .OrderByDescending(x => x.Missed)
        .ThenBy(x => x.File, StringComparer.Ordinal)
        .ToArray();

    Console.WriteLine($"Top {Math.Min(top, fileStats.Length)} files by missed lines:");
    foreach (var s in fileStats.Take(top))
    {
        var pct = (double)s.Covered / s.Total * 100.0;
        Console.WriteLine($"{s.Missed,5} missed / {s.Total,5} lines ({pct,6:0.0}%) | {s.File}");
    }
}
