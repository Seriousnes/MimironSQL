using Microsoft.EntityFrameworkCore;

using MimironSQL;
using MimironSQL.EntityFrameworkCore;
using MimironSQL.Providers;

var scenario = GetArgValue(args, "--scenario") ?? "help";
var iterations = TryGetIntArg(args, "--iterations", 10);
var warmup = TryGetIntArg(args, "--warmup", 1);

switch (scenario)
{
    case "casc-spell":
        await RunCascSpellQueryAsync(iterations, warmup);
        return 0;

    case "help":
    default:
        PrintHelp();
        return scenario == "help" ? 0 : 2;
}

static async Task RunCascSpellQueryAsync(int iterations, int warmup)
{
    if (!TryGetWowInstallRoot(out var wowInstallRoot) || !Directory.Exists(wowInstallRoot))
        throw new InvalidOperationException("WOW install root not found. Create .env.local with WOW_INSTALL_ROOT=... (same as integration tests).");

    var testDataDir = GetTestDataDirectory();
    if (!Directory.Exists(testDataDir))
        throw new DirectoryNotFoundException(testDataDir);

    var optionsBuilder = new DbContextOptionsBuilder<WoWDb2Context>();
    optionsBuilder.UseMimironDb2(o => o
            .WithWowVersion("12.0.0.65655")
            .UseCasc()
            .WithWowInstallRoot(wowInstallRoot)
            .WithDbdDefinitions(Path.Combine(testDataDir, "definitions"))
            .WithManifest(testDataDir, "manifest.json")
            .Apply());

    using var context = new WoWDb2Context(optionsBuilder.Options);
    GC.KeepAlive(context.Model); // Force model initialization before profiling.

    Func<SpellEntity?> querySpell = () => context.Spell.SingleOrDefault(x => x.Id == 454009);

    for (int i = 0; i < warmup; i++)
        querySpell();

    // Attach the CPU profiler to this process and start collection after you see this message.
    Console.WriteLine($"Profiling: scenario=casc-spell warmup={warmup} iterations={iterations}. Press any key to continue...");
    //Console.ReadKey();

    for (int i = 0; i < iterations; i++)
        querySpell();

    await Task.CompletedTask;
}

static string GetTestDataDirectory()
    => Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "TestData"));

static bool TryGetWowInstallRoot(out string wowInstallRoot)
{
    wowInstallRoot = string.Empty;
    var path = GetEnvLocalPath();
    if (!File.Exists(path))
        return false;

    foreach (var line in File.ReadLines(path))
    {
        var trimmed = line.Trim();
        if (trimmed.Length == 0)
            continue;

        if (trimmed.StartsWith('#'))
            continue;

        var equals = trimmed.IndexOf('=');
        if (equals <= 0)
            continue;

        var key = trimmed[..equals].Trim();
        if (!string.Equals(key, "WOW_INSTALL_ROOT", StringComparison.OrdinalIgnoreCase))
            continue;

        var value = trimmed[(equals + 1)..].Trim();
        wowInstallRoot = TrimOptionalQuotes(value);
        return wowInstallRoot.Length > 0;
    }

    return false;
}

static string GetEnvLocalPath()
    => Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".env.local"));

static string TrimOptionalQuotes(string value)
    => value is ['\"', .., '\"'] or ['\'', .., '\''] ? value[1..^1].Trim() : value;

static void PrintHelp()
{
    Console.WriteLine("MimironSQL.Profiling");
    Console.WriteLine();
    Console.WriteLine("Usage:");
    Console.WriteLine("  dotnet run -c Release -p tests/MimironSQL.Profiling -- --scenario casc-spell [--warmup N] [--iterations N]");
    Console.WriteLine();
    Console.WriteLine("Scenarios:");
    Console.WriteLine("  casc-spell   Query SpellEntity(Id=454009) via CASC provider (similar to integration test)");
}

static string? GetArgValue(string[] args, string name)
{
    for (int i = 0; i < args.Length; i++)
    {
        if (!string.Equals(args[i], name, StringComparison.OrdinalIgnoreCase))
            continue;

        if (i + 1 < args.Length)
            return args[i + 1];
    }

    return null;
}

static int TryGetIntArg(string[] args, string name, int defaultValue)
{
    var value = GetArgValue(args, name);
    return int.TryParse(value, out var i) && i >= 0 ? i : defaultValue;
}
