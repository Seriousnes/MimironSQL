using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace MimironSQL.Formats.Wdc5.Db2;

/// <summary>
/// Represents a WDC5 section header.
/// </summary>
/// <param name="TactKeyLookup">TACT key lookup identifier (0 for unencrypted sections).</param>
/// <param name="FileOffset">File offset where the section begins.</param>
/// <param name="NumRecords">Number of records in the section.</param>
/// <param name="StringTableSize">String table size for the section in bytes.</param>
/// <param name="OffsetRecordsEndOffset">Absolute file offset where record data ends (sparse sections).</param>
/// <param name="IndexDataSize">Index data size in bytes.</param>
/// <param name="ParentLookupDataSize">Parent lookup data size in bytes.</param>
/// <param name="OffsetMapIDCount">Offset map ID count.</param>
/// <param name="CopyTableCount">Copy table entry count.</param>
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
