using Microsoft.CodeAnalysis.Diagnostics;

namespace MimironSQL.DbContextGenerator;

internal sealed class GeneratorOptions(string wowDbDefsRoot)
{
    public string WowDbDefsRoot { get; } = wowDbDefsRoot;

    public static GeneratorOptions From(AnalyzerConfigOptions globalOptions)
    {
        globalOptions.TryGetValue("build_property.CascNetWowDbDefsRoot", out var wowDbDefsRoot);

        return new GeneratorOptions(
            wowDbDefsRoot?.Trim() ?? string.Empty);
    }
}
