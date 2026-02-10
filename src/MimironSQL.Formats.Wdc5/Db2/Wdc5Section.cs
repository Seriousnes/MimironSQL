namespace MimironSQL.Formats.Wdc5.Db2;

/// <summary>
/// Represents a parsed WDC5 section and its associated data blocks.
/// </summary>
public sealed class Wdc5Section
{
    /// <summary>
    /// Gets the raw section header.
    /// </summary>
    public required Wdc5SectionHeader Header { get; init; }

    /// <summary>
    /// Gets the global record index of the first record in this section.
    /// </summary>
    public required int FirstGlobalRecordIndex { get; init; }

    /// <summary>
    /// Gets the raw bytes containing the section's record data.
    /// </summary>
    public required byte[] RecordsData { get; init; }

    /// <summary>
    /// Gets the record data size in bytes.
    /// </summary>
    public required int RecordDataSizeBytes { get; init; }

    /// <summary>
    /// Gets the base offset within the concatenated record blob.
    /// </summary>
    public required int RecordsBaseOffsetInBlob { get; init; }

    /// <summary>
    /// Gets the TACT key bytes used to decrypt the section, if available.
    /// </summary>
    public ReadOnlyMemory<byte> TactKey { get; init; } = ReadOnlyMemory<byte>.Empty;

    /// <summary>
    /// Gets the base offset of this section's string table within the dense string table.
    /// </summary>
    public int StringTableBaseOffset { get; init; } = 0;

    /// <summary>
    /// Gets the raw string table bytes for the section.
    /// </summary>
    public byte[] StringTableBytes { get; init; } = [];

    /// <summary>
    /// Gets the index data block, if present.
    /// </summary>
    public int[] IndexData { get; init; } = [];

    /// <summary>
    /// Gets the copy data mapping (destination ID to source ID), if present.
    /// </summary>
    public Dictionary<int, int> CopyData { get; init; } = [];

    /// <summary>
    /// Gets parent lookup entries, if present.
    /// </summary>
    public Dictionary<int, int> ParentLookupEntries { get; init; } = [];

    /// <summary>
    /// Gets sparse record entries, if the file is sparse.
    /// </summary>
    public SparseEntry[] SparseEntries { get; init; } = [];

    /// <summary>
    /// Gets the bit-start positions for sparse records.
    /// </summary>
    public int[] SparseRecordStartBits { get; init; } = [];

    /// <summary>
    /// Gets a value indicating whether the section is encrypted.
    /// </summary>
    public bool IsEncrypted => Header is { TactKeyLookup: not 0 };

    /// <summary>
    /// Gets a value indicating whether the section can be decrypted with the available key.
    /// </summary>
    public bool IsDecryptable => IsEncrypted && !TactKey.IsEmpty;

    /// <summary>
    /// Gets the number of records in the section.
    /// </summary>
    public int NumRecords => Header.NumRecords;

    /// <summary>
    /// Gets a value indicating whether index data is present.
    /// </summary>
    public bool HasIndexData => Header is { IndexDataSize: > 0 };

    /// <summary>
    /// Gets a value indicating whether parent lookup data is present.
    /// </summary>
    public bool HasParentLookupData => Header is { ParentLookupDataSize: > 0 };

    /// <summary>
    /// Builds the per-record bit start positions for sparse record data.
    /// </summary>
    /// <param name="entries">Sparse entries describing each record.</param>
    /// <param name="sectionFileOffset">File offset where the section's record data begins.</param>
    /// <param name="recordDataSizeBytes">Total record data size in bytes.</param>
    /// <returns>An array of bit start positions, one per entry.</returns>
    public static int[] BuildSparseRecordStartBits(SparseEntry[] entries, int sectionFileOffset, int recordDataSizeBytes)
    {
        if (entries is { Length: 0 })
            return [];

        if (recordDataSizeBytes < 0)
            throw new InvalidDataException("Sparse record data size is negative.");

        // Some files appear to store sparse records contiguously and leave the Offset column unused (0).
        // Other files may provide meaningful absolute offsets; support both but fail loudly on inconsistent data.
        var hasAnyNonZeroOffset = false;
        for (var i = 0; i < entries.Length; i++)
        {
            if (entries[i] is { Offset: not 0 })
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
            var startBytes = entries[i].Offset - sectionFileOffset;
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
