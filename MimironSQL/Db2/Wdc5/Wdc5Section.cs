using MimironSQL.Db2;
using System;
using System.Collections.Generic;

namespace MimironSQL.Db2.Wdc5;

public sealed class Wdc5Section
{
    public required Wdc5SectionHeader Header { get; init; }
    public required int FirstGlobalRecordIndex { get; init; }
    public required byte[] RecordsData { get; init; }
    public required int RecordDataSizeBytes { get; init; }
    public required int RecordsBaseOffsetInBlob { get; init; }

    public ReadOnlyMemory<byte> TactKey { get; init; } = ReadOnlyMemory<byte>.Empty;

    public int StringTableBaseOffset { get; init; } = 0;
    public byte[] StringTableBytes { get; init; } = Array.Empty<byte>();
    public int[] IndexData { get; init; } = Array.Empty<int>();
    public Dictionary<int, int> CopyData { get; init; } = new();
    public SparseEntry[] SparseEntries { get; init; } = Array.Empty<SparseEntry>();
    public int[] SparseRecordStartBits { get; init; } = Array.Empty<int>();

    public bool IsEncrypted => Header.TactKeyLookup != 0;
    public bool IsDecryptable => IsEncrypted && !TactKey.IsEmpty;
    public int NumRecords => Header.NumRecords;
    public bool HasIndexData => Header.IndexDataSize > 0;

    public static int[] BuildSparseRecordStartBits(SparseEntry[] entries, int sectionFileOffset, int recordDataSizeBytes)
    {
        if (entries.Length == 0)
            return Array.Empty<int>();

        if (recordDataSizeBytes < 0)
            throw new InvalidDataException("Sparse record data size is negative.");

        // Some files appear to store sparse records contiguously and leave the Offset column unused (0).
        // Other files may provide meaningful absolute offsets; support both but fail loudly on inconsistent data.
        var hasAnyNonZeroOffset = false;
        for (var i = 0; i < entries.Length; i++)
        {
            if (entries[i].Offset != 0)
            {
                hasAnyNonZeroOffset = true;
                break;
            }
        }

        var starts = new int[entries.Length];

        if (!hasAnyNonZeroOffset)
        {
            var bitPosition = 0;
            for (var i = 0; i < entries.Length; i++)
            {
                starts[i] = bitPosition;
                bitPosition += entries[i].Size * 8;
            }

            var totalBytes = (bitPosition + 7) >> 3;
            if (totalBytes > recordDataSizeBytes)
                throw new InvalidDataException("Sparse entry sizes exceed available record data.");

            return starts;
        }

        long previousStartBytes = -1;
        for (var i = 0; i < entries.Length; i++)
        {
            var startBytes = (long)entries[i].Offset - sectionFileOffset;
            if (startBytes < 0 || startBytes > recordDataSizeBytes)
                throw new InvalidDataException("Sparse entry offset is outside the section record data.");

            if (startBytes < previousStartBytes)
                throw new InvalidDataException("Sparse entry offsets are not sorted.");

            var endBytes = startBytes + entries[i].Size;
            if (endBytes < startBytes || endBytes > recordDataSizeBytes)
                throw new InvalidDataException("Sparse entry extends outside the section record data.");

            starts[i] = checked((int)startBytes * 8);
            previousStartBytes = startBytes;
        }

        return starts;
    }
}
