using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace MimironSQL.Formats.Wdc5.Db2;

[ExcludeFromCodeCoverage]
[StructLayout(LayoutKind.Sequential, Pack = 2)]
public readonly record struct SparseEntry(uint Offset, ushort Size);
