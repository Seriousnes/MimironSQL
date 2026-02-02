using Microsoft.CodeAnalysis.Diagnostics;

namespace CASC.Net.Generators;

internal sealed record GeneratorOptions(
    string WowDbDefsRoot)
{
    public static GeneratorOptions From(AnalyzerConfigOptions globalOptions)
    {
        globalOptions.TryGetValue("build_property.CascNetWowDbDefsRoot", out var wowDbDefsRoot);

        return new GeneratorOptions(
            WowDbDefsRoot: wowDbDefsRoot?.Trim() ?? string.Empty);
    }
}
