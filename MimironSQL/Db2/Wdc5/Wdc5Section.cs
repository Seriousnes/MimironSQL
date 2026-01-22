using MimironSQL.Db2;
using System;
using System.Collections.Generic;

namespace MimironSQL.Db2.Wdc5;

public sealed class Wdc5Section
{
    public required Wdc5SectionHeader Header { get; init; }
    public required byte[] RecordsData { get; init; }

    public byte[] StringTableBytes { get; init; } = Array.Empty<byte>();
    public int[] IndexData { get; init; } = Array.Empty<int>();
    public Dictionary<int, int> CopyData { get; init; } = new();
    public SparseEntry[] SparseEntries { get; init; } = Array.Empty<SparseEntry>();
    public int[] SparseRecordStartBits { get; init; } = Array.Empty<int>();

    public bool IsEncrypted => Header.TactKeyLookup != 0;
    public int NumRecords => Header.NumRecords;
    public bool HasIndexData => Header.IndexDataSize > 0;

    public static int[] BuildSparseRecordStartBits(SparseEntry[] entries)
    {
        if (entries.Length == 0)
            return Array.Empty<int>();

        var starts = new int[entries.Length];
        var bitPosition = 0;
        for (var i = 0; i < entries.Length; i++)
        {
            starts[i] = bitPosition;
            bitPosition += entries[i].Size * 8;
        }

        return starts;
    }
}
