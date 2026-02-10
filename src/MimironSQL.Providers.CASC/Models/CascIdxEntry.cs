using System.Diagnostics.CodeAnalysis;

namespace MimironSQL.Providers;

/// <summary>
/// Represents a single entry in a CASC <c>.idx</c> file.
/// </summary>
/// <param name="KeyPrefix">The key prefix used for lookups.</param>
/// <param name="ArchiveIndex">The archive index (for example <c>data.###</c>).</param>
/// <param name="Offset">The byte offset within the archive.</param>
/// <param name="Size">The encoded record size in bytes.</param>
[ExcludeFromCodeCoverage]
internal readonly record struct CascIdxEntry(ReadOnlyMemory<byte> KeyPrefix, int ArchiveIndex, long Offset, uint Size);
