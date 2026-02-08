using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace MimironSQL.Formats.Wdc5.Db2;

[ExcludeFromCodeCoverage]
[StructLayout(LayoutKind.Sequential, Pack = 2)]
public readonly record struct Wdc5SectionHeader(
    ulong TactKeyLookup,
    int FileOffset,
    int NumRecords,
    int StringTableSize,
    int OffsetRecordsEndOffset,
    int IndexDataSize,
    int ParentLookupDataSize,
    int OffsetMapIDCount,
    int CopyTableCount);
