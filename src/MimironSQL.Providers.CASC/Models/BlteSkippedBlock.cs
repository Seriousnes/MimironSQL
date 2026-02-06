using System.Diagnostics.CodeAnalysis;

namespace MimironSQL.Providers;

[ExcludeFromCodeCoverage]
public readonly record struct BlteSkippedBlock(int BlockIndex, uint RawSize, uint LogicalSize, char Mode);
