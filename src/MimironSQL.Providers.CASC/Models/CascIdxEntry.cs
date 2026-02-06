using System.Diagnostics.CodeAnalysis;

namespace MimironSQL.Providers;

[ExcludeFromCodeCoverage]
public readonly record struct CascIdxEntry(ReadOnlyMemory<byte> KeyPrefix, int ArchiveIndex, long Offset, uint Size);
