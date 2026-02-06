using System.Globalization;
using System.Diagnostics;
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

static string FindRepoRoot(string startDir)
{
    var current = new DirectoryInfo(Path.GetFullPath(startDir));
    while (current is not null)
    {
        if (File.Exists(Path.Combine(current.FullName, "MimironSQL.slnx")))
            return current.FullName;

        current = current.Parent;
    }

    return Path.GetFullPath(startDir);
}

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

static string? FindLatestSummary(string repoRoot)
{
    var history = Path.Combine(repoRoot, "coverage", "history");
    if (!Directory.Exists(history))
        return null;

    var candidates = Directory.EnumerateDirectories(history)
        .Select(d => new
        {
            Name = Path.GetFileName(d),
            Summary = Path.Combine(d, "merged", "Summary.txt"),
        })
        .Where(x => File.Exists(x.Summary))
        .OrderByDescending(x => x.Name, StringComparer.Ordinal)
        .ToArray();

    return candidates.FirstOrDefault()?.Summary;
}

static int ParseInt(string? value)
    => int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : 0;

static void PrintUsage()
{
    Console.WriteLine("Usage:");
    Console.WriteLine("  dotnet run .\\tools\\coverage\\reportcoverage.cs -- [--skip-tests] [--no-build] [--filter <substring>] [--top <N>] [--files] [--percent]");
    Console.WriteLine("  dotnet run .\\tools\\coverage\\reportcoverage.cs -- [--skip-tests] [--no-build] --class <substring> [--methods] [--missed-lines] [--top <N>]");
    Console.WriteLine();
    Console.WriteLine("Defaults:");
    Console.WriteLine("  default mode runs: dotnet test + reportgenerator + analysis");
    Console.WriteLine("  --top: 30");
    Console.WriteLine();
    Console.WriteLine("Modes:");
    Console.WriteLine("  (default)           run dotnet test + reportgenerator, then analyze merged Cobertura.xml");
    Console.WriteLine("  --skip-tests         analyze the latest coverage/history/*/merged/Cobertura.xml (no test run, no merge)");
    Console.WriteLine("  --no-build           pass --no-build through to dotnet test (default mode only)");
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

static int RunProcess(string fileName, string arguments, string workingDirectory)
{
    using var p = new Process();
    p.StartInfo = new ProcessStartInfo
    {
        FileName = fileName,
        Arguments = arguments,
        WorkingDirectory = workingDirectory,
        UseShellExecute = false,
        RedirectStandardOutput = true,
        RedirectStandardError = true,
    };

    p.OutputDataReceived += (_, e) =>
    {
        if (e.Data is not null)
            Console.WriteLine(e.Data);
    };

    p.ErrorDataReceived += (_, e) =>
    {
        if (e.Data is not null)
            Console.Error.WriteLine(e.Data);
    };

    if (!p.Start())
        return 1;

    p.BeginOutputReadLine();
    p.BeginErrorReadLine();
    p.WaitForExit();
    return p.ExitCode;
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

var repoRoot = FindRepoRoot(Directory.GetCurrentDirectory());

var filter = TryGetArg(cliArgs, "--filter");
var classFilter = TryGetArg(cliArgs, "--class");
var topText = TryGetArg(cliArgs, "--top");
var top = int.TryParse(topText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedTop) ? parsedTop : 30;
if (top <= 0)
    top = 30;

var skipTests = HasFlag(cliArgs, "--skip-tests");
var noBuild = HasFlag(cliArgs, "--no-build");
var showFiles = HasFlag(cliArgs, "--files");
var sortByPercent = HasFlag(cliArgs, "--percent");
var showMethods = HasFlag(cliArgs, "--methods");
var showMissedLines = HasFlag(cliArgs, "--missed-lines");

string? coberturaPath;
if (skipTests)
{
    coberturaPath = FindLatestCobertura(repoRoot);
    if (coberturaPath is null)
    {
        Console.Error.WriteLine("No Cobertura.xml found under coverage/history. Run without --skip-tests first.");
        Environment.ExitCode = 1;
        return;
    }

    var summaryPath = FindLatestSummary(repoRoot);
    if (summaryPath is not null)
    {
        Console.WriteLine($"Summary:   {summaryPath}");
        Console.WriteLine();
        Console.WriteLine(File.ReadAllText(summaryPath));
    }
}
else
{
    var ts = DateTime.Now.ToString("yyyyMMdd-HHmmss", CultureInfo.InvariantCulture);
    var runDir = Path.Combine(repoRoot, "coverage", "history", ts);
    var resultsDir = Path.Combine(runDir, "testresults");
    var mergedDir = Path.Combine(runDir, "merged");
    Directory.CreateDirectory(resultsDir);
    Directory.CreateDirectory(mergedDir);

    var slnxPath = Path.Combine(repoRoot, "MimironSQL.slnx");
    var runSettings = Path.Combine(repoRoot, "tests", "coverlet.runsettings");

    Console.WriteLine($"Repo root: {repoRoot}");
    Console.WriteLine($"Run dir:   {runDir}");
    Console.WriteLine();

    var noBuildArg = noBuild ? " --no-build" : string.Empty;
    var dotnetArgs = $"test .\\MimironSQL.slnx --configuration Release --collect:\"XPlat Code Coverage\" --settings .\\tests\\coverlet.runsettings --results-directory \"{resultsDir}\"{noBuildArg}";
    Console.WriteLine($"> dotnet {dotnetArgs}");
    var testExit = RunProcess("dotnet", dotnetArgs, workingDirectory: repoRoot);
    if (testExit != 0)
    {
        Environment.ExitCode = testExit;
        return;
    }

    var reportArgs = $"-reports:\"{resultsDir}\\**\\coverage.cobertura.xml\" -targetdir:\"{mergedDir}\" -reporttypes:\"Cobertura;TextSummary\"";
    Console.WriteLine();
    Console.WriteLine($"> reportgenerator {reportArgs}");
    var reportExit = RunProcess("reportgenerator", reportArgs, workingDirectory: repoRoot);
    if (reportExit != 0)
    {
        Environment.ExitCode = reportExit;
        return;
    }

    var summary = Path.Combine(mergedDir, "Summary.txt");
    if (File.Exists(summary))
    {
        Console.WriteLine();
        Console.WriteLine(File.ReadAllText(summary));
    }

    coberturaPath = Path.Combine(mergedDir, "Cobertura.xml");
    if (!File.Exists(coberturaPath))
    {
        Console.Error.WriteLine($"Cobertura.xml not found at '{coberturaPath}'.");
        Environment.ExitCode = 1;
        return;
    }
}

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
        var exactNameMatches = matches
            .Where(m => string.Equals(m.Name, classFilter, StringComparison.OrdinalIgnoreCase))
            .ToArray();

        if (exactNameMatches.Length == 1)
        {
            matches = exactNameMatches;
        }
        else
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
