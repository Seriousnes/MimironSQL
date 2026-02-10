using System.Diagnostics.CodeAnalysis;

namespace MimironSQL.Providers;

[ExcludeFromCodeCoverage]
/// <summary>
/// Describes the sizes of fields within an <c>.idx</c> entry record.
/// </summary>
/// <param name="Size">The size field length in bytes.</param>
/// <param name="Offset">The offset field length in bytes.</param>
/// <param name="Key">The key prefix field length in bytes.</param>
/// <param name="OffsetBits">The number of low bits used for the offset within the archive.</param>
internal readonly record struct CascIdxHeaderSpec(
    byte Size,
    byte Offset,
    byte Key,
    byte OffsetBits);
