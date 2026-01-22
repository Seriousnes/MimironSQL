using MimironSQL.Db2;
using System;
using System.Collections.Generic;

namespace MimironSQL.Db2.Wdc5;

public sealed class Wdc5Section
{
    public required Wdc5SectionHeader Header { get; init; }
    public required int FirstGlobalRecordIndex { get; init; }
    public required byte[] RecordsData { get; init; }

    public int StringTableBaseOffset { get; init; } = 0;
    public byte[] StringTableBytes { get; init; } = Array.Empty<byte>();
    public int[] IndexData { get; init; } = Array.Empty<int>();
    public Dictionary<int, int> CopyData { get; init; } = new();
    public SparseEntry[] SparseEntries { get; init; } = Array.Empty<SparseEntry>();
    public int[] SparseRecordStartBits { get; init; } = Array.Empty<int>();

    public bool IsEncrypted => Header.TactKeyLookup != 0;
    public int NumRecords => Header.NumRecords;
    public bool HasIndexData => Header.IndexDataSize > 0;

    public static int[] BuildSparseRecordStartBits(SparseEntry[] entries, int sectionFileOffset)
    {
        if (entries.Length == 0)
            return Array.Empty<int>();

        var starts = new int[entries.Length];
        for (var i = 0; i < entries.Length; i++)
        {
            var relativeOffsetBytes = (long)entries[i].Offset - sectionFileOffset;
            if (relativeOffsetBytes < 0 || relativeOffsetBytes > int.MaxValue)
                throw new InvalidOperationException("Sparse offset map entry offset is out of range for this section.");

            starts[i] = checked((int)relativeOffsetBytes * 8);
        }

        return starts;
    }
}
