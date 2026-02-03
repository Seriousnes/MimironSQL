using System.Collections.Immutable;

using Microsoft.CodeAnalysis.Diagnostics;

namespace MimironSQL.DbContextGenerator.Tests.Helpers;

internal sealed class TestAnalyzerConfigOptions(ImmutableDictionary<string, string> options) : AnalyzerConfigOptions
{
    private readonly ImmutableDictionary<string, string> _options = options;

    public static AnalyzerConfigOptions Empty { get; } = new TestAnalyzerConfigOptions(ImmutableDictionary<string, string>.Empty);

    public override bool TryGetValue(string key, out string value)
        => _options.TryGetValue(key, out value!);
}
