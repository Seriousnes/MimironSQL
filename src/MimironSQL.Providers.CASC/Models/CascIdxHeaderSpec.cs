using System.Diagnostics.CodeAnalysis;

namespace MimironSQL.Providers;

[ExcludeFromCodeCoverage]
public readonly record struct CascIdxHeaderSpec(
    byte Size,
    byte Offset,
    byte Key,
    byte OffsetBits);
