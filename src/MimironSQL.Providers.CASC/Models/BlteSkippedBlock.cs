using System.Diagnostics.CodeAnalysis;

namespace MimironSQL.Providers;

/// <summary>
/// Describes a BLTE block that was skipped during decoding.
/// </summary>
/// <param name="BlockIndex">The zero-based block index.</param>
/// <param name="RawSize">The raw encoded block size in bytes.</param>
/// <param name="LogicalSize">The logical decoded size in bytes.</param>
/// <param name="Mode">The BLTE block mode character.</param>
[ExcludeFromCodeCoverage]
internal readonly record struct BlteSkippedBlock(int BlockIndex, uint RawSize, uint LogicalSize, char Mode);
