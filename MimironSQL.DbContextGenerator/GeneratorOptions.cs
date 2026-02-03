using Microsoft.CodeAnalysis.Diagnostics;

namespace MimironSQL.DbContextGenerator;

internal sealed class GeneratorOptions
{
	public string WowDbDefsRoot { get; }

	public GeneratorOptions(string wowDbDefsRoot)
	{
		WowDbDefsRoot = wowDbDefsRoot;
	}

    public static GeneratorOptions From(AnalyzerConfigOptions globalOptions)
    {
        globalOptions.TryGetValue("build_property.CascNetWowDbDefsRoot", out var wowDbDefsRoot);

        return new GeneratorOptions(
            wowDbDefsRoot?.Trim() ?? string.Empty);
    }
}
