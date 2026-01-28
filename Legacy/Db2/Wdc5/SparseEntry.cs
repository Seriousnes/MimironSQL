using System.Runtime.InteropServices;

namespace MimironSQL.Db2.Wdc5;

[StructLayout(LayoutKind.Sequential, Pack = 2)]
public readonly record struct SparseEntry(uint Offset, ushort Size);
