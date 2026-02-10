using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace MimironSQL.Formats.Wdc5.Db2;

/// <summary>
/// Describes a sparse record entry within a WDC5 section.
/// </summary>
/// <param name="Offset">Absolute file offset of the record data.</param>
/// <param name="Size">Record size in bytes.</param>
[ExcludeFromCodeCoverage]
[StructLayout(LayoutKind.Sequential, Pack = 2)]
public readonly record struct SparseEntry(uint Offset, ushort Size);
