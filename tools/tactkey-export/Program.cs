using System.Globalization;
using System.Text;

using Microsoft.EntityFrameworkCore;

using MimironSQL;
using MimironSQL.EntityFrameworkCore;
using MimironSQL.Providers;

var envValues = LoadEnvFile();

var wowVersion = GetRequiredValue(envValues, "WOW_VERSION");
var wowInstallRoot = GetRequiredValue(envValues, "WOW_INSTALL_ROOT");

Console.WriteLine($"WoW version:  {wowVersion}");
Console.WriteLine($"Install root: {wowInstallRoot}");
Console.WriteLine();

var baseDir = AppContext.BaseDirectory;
var definitionsDir = Path.Combine(baseDir, "definitions");
var manifestPath = Path.Combine(baseDir, "manifest.json");

var optionsBuilder = new DbContextOptionsBuilder<TactKeyDb2Context>();
optionsBuilder.UseMimironDb2(o =>
{
    o.WithWowVersion(wowVersion);
    o.UseCasc(casc =>
    {
        casc.WowInstallRoot = wowInstallRoot;
        casc.DbdDefinitionsDirectory = definitionsDir;
        casc.ManifestDirectory = baseDir;
        casc.ManifestAssetName = "manifest.json";
    });
});

using var context = new TactKeyDb2Context(optionsBuilder.Options);

// Force model creation so the provider initialises.
_ = context.Model;

var keys = context.TactKey.ToDictionary(k => k.Id, k => k.Key);
var lookups = context.TactKeyLookup.ToList();

Console.WriteLine($"Loaded {keys.Count} TactKey rows and {lookups.Count} TactKeyLookup rows.");
Console.WriteLine();

var sb = new StringBuilder();

foreach (var lookup in lookups.OrderBy(l => FormatHex(l.TACTID), StringComparer.OrdinalIgnoreCase))
{
    if (!keys.TryGetValue(lookup.Id, out var keyBytes))
        continue;

    sb.Append(FormatHex(lookup.TACTID));
    sb.Append(' ');
    sb.AppendLine(FormatHex(keyBytes));
}

var outputPath = Path.Combine(Environment.CurrentDirectory, "WoW.txt");
File.WriteAllText(outputPath, sb.ToString());
Console.WriteLine($"Exported {lookups.Count} entries to {outputPath}");

// ------- helpers -------

static string FormatHex(byte[] bytes)
    => Convert.ToHexString(bytes);

static Dictionary<string, string> LoadEnvFile()
{
    var envPath = Path.Combine(AppContext.BaseDirectory, ".env");
    if (!File.Exists(envPath))
    {
        // Fall back to project directory (running via dotnet run).
        envPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".env"));
    }

    if (!File.Exists(envPath))
        throw new FileNotFoundException($".env file not found. Expected at '{envPath}'.");

    var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    foreach (var line in File.ReadLines(envPath)
                 .Select(l => l.Trim())
                 .Where(l => l.Length > 0 && !l.StartsWith('#')))
    {
        var equals = line.IndexOf('=');
        if (equals <= 0)
            continue;

        var key = line[..equals].Trim();
        var value = line[(equals + 1)..].Trim();

        if (value is ['"', .., '"'] or ['\'', .., '\''])
            value = value[1..^1].Trim();

        values[key] = value;
    }

    return values;
}

static string GetRequiredValue(Dictionary<string, string> env, string key)
{
    if (env.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value))
        return value;

    throw new InvalidOperationException($"Required .env variable '{key}' is not set.");
}
