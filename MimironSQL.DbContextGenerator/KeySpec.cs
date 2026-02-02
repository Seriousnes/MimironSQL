using System.Collections.Immutable;

namespace CASC.Net.Generators;

internal sealed record KeySpec(ImmutableArray<string> ColumnNames);
