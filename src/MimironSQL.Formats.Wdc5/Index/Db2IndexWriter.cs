namespace MimironSQL.Formats.Wdc5.Index;

internal static class Db2IndexWriter
{
    public static void Write(
        string filePath,
        IReadOnlyList<(ulong EncodedValue, ushort SectionIndex, int RowIndex)> sortedEntries,
        byte valueByteWidth,
        string tableName,
        int fieldIndex,
        byte valueType,
        string wowVersion,
        uint layoutHash)
    {
        var n = sortedEntries.Count;
        var maxLeafEntries = Db2IndexFileFormat.GetMaxLeafEntries(valueByteWidth);
        var leafPageCount = n == 0 ? 0 : (n + maxLeafEntries - 1) / maxLeafEntries;

        var leafPageInfos = new List<(ulong FirstKey, long PageOffset)>(leafPageCount);
        for (var i = 0; i < leafPageCount; i++)
        {
            leafPageInfos.Add((sortedEntries[i * maxLeafEntries].EncodedValue, (long)Db2IndexFileFormat.PageSize + (long)i * Db2IndexFileFormat.PageSize));
        }

        var (rootPageOffset, treeHeight, totalInternalPageCount) = ComputeTreeShape(leafPageInfos, leafPageCount, valueByteWidth);
        var totalPages = 1 + leafPageCount + totalInternalPageCount;

        var tempPath = filePath + ".tmp";
        try
        {
            using (var stream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, Db2IndexFileFormat.PageSize, FileOptions.SequentialScan))
            {
                stream.SetLength((long)totalPages * Db2IndexFileFormat.PageSize);

                var buf = new byte[Db2IndexFileFormat.PageSize];

                WriteHeader(stream, buf, tableName, fieldIndex, valueType, valueByteWidth,
                    wowVersion, layoutHash, rootPageOffset, n, treeHeight);

                for (var li = 0; li < leafPageCount; li++)
                {
                    var startEntry = li * maxLeafEntries;
                    var count = Math.Min(maxLeafEntries, n - startEntry);
                    var nextLeafOffset = li < leafPageCount - 1
                        ? leafPageInfos[li + 1].PageOffset
                        : 0L;

                    WriteLeafPage(stream, buf, sortedEntries, startEntry, count, valueByteWidth,
                        leafPageInfos[li].PageOffset, nextLeafOffset);
                }

                var currentLevel = leafPageInfos;
                var nextInternalOffset = (long)Db2IndexFileFormat.PageSize + (long)leafPageCount * Db2IndexFileFormat.PageSize;

                while (currentLevel.Count > 1)
                {
                    var maxInternalKeys = Db2IndexFileFormat.GetMaxInternalKeys(valueByteWidth);
                    var nextLevel = new List<(ulong FirstKey, long PageOffset)>();
                    var i = 0;
                    while (i < currentLevel.Count)
                    {
                        var chunkSize = Math.Min(maxInternalKeys + 1, currentLevel.Count - i);
                        var internalPageOffset = nextInternalOffset;
                        nextInternalOffset += Db2IndexFileFormat.PageSize;

                        WriteInternalPage(stream, buf, currentLevel, i, chunkSize, valueByteWidth, internalPageOffset);
                        nextLevel.Add((currentLevel[i].FirstKey, internalPageOffset));
                        i += chunkSize;
                    }
                    currentLevel = nextLevel;
                }

                stream.Flush();
            }

            File.Move(tempPath, filePath, overwrite: true);
        }
        catch
        {
            try
            {
                File.Delete(tempPath);
            }
            catch
            {
            }

            throw;
        }
    }

    private static (long RootPageOffset, int TreeHeight, int TotalInternalPageCount) ComputeTreeShape(
        List<(ulong FirstKey, long PageOffset)> leafPageInfos,
        int leafPageCount,
        byte valueByteWidth)
    {
        if (leafPageCount == 0)
        {
            return (Db2IndexFileFormat.EmptyRootOffset, 0, 0);
        }

        if (leafPageCount == 1)
        {
            return (leafPageInfos[0].PageOffset, 1, 0);
        }

        var maxInternalKeys = Db2IndexFileFormat.GetMaxInternalKeys(valueByteWidth);
        var treeHeight = 1;
        var totalInternal = 0;
        var currentCount = leafPageCount;

        while (currentCount > 1)
        {
            treeHeight++;
            var internalNodeCount = (currentCount + maxInternalKeys) / (maxInternalKeys + 1);
            totalInternal += internalNodeCount;
            currentCount = internalNodeCount;
        }

        var currentLevel = leafPageInfos;
        var nextOffset = (long)Db2IndexFileFormat.PageSize + (long)leafPageCount * Db2IndexFileFormat.PageSize;
        long rootOffset = 0;

        while (currentLevel.Count > 1)
        {
            var nextLevel = new List<(ulong FirstKey, long PageOffset)>();
            var i = 0;
            while (i < currentLevel.Count)
            {
                var chunkSize = Math.Min(maxInternalKeys + 1, currentLevel.Count - i);
                var internalPageOffset = nextOffset;
                nextOffset += Db2IndexFileFormat.PageSize;
                nextLevel.Add((currentLevel[i].FirstKey, internalPageOffset));
                rootOffset = internalPageOffset;
                i += chunkSize;
            }
            currentLevel = nextLevel;
        }

        return (rootOffset, treeHeight, totalInternal);
    }

    private static void WriteHeader(
        FileStream stream, byte[] buf,
        string tableName, int fieldIndex,
        byte valueType, byte valueByteWidth,
        string wowVersion, uint layoutHash,
        long rootPageOffset, int recordCount, int treeHeight)
    {
        Array.Clear(buf, 0, Db2IndexFileFormat.PageSize);
        Db2IndexFileFormat.WriteUInt32(buf, Db2IndexFileFormat.HdrMagicOffset, Db2IndexFileFormat.Magic);
        Db2IndexFileFormat.WriteInt32(buf, Db2IndexFileFormat.HdrVersionOffset, Db2IndexFileFormat.FormatVersion);
        Db2IndexFileFormat.WriteFixedLengthString(buf, Db2IndexFileFormat.HdrWowVersionOffset, Db2IndexFileFormat.WowVersionMaxBytes, wowVersion);
        Db2IndexFileFormat.WriteUInt32(buf, Db2IndexFileFormat.HdrLayoutHashOffset, layoutHash);
        Db2IndexFileFormat.WriteFixedLengthString(buf, Db2IndexFileFormat.HdrTableNameOffset, Db2IndexFileFormat.TableNameMaxBytes, tableName);
        Db2IndexFileFormat.WriteInt32(buf, Db2IndexFileFormat.HdrFieldIndexOffset, fieldIndex);
        buf[Db2IndexFileFormat.HdrValueTypeOffset] = valueType;
        buf[Db2IndexFileFormat.HdrValueByteWidthOffset] = valueByteWidth;
        Db2IndexFileFormat.WriteInt32(buf, Db2IndexFileFormat.HdrPageSizeOffset, Db2IndexFileFormat.PageSize);
        Db2IndexFileFormat.WriteInt64(buf, Db2IndexFileFormat.HdrRootPageOffsetOffset, rootPageOffset);
        Db2IndexFileFormat.WriteInt32(buf, Db2IndexFileFormat.HdrRecordCountOffset, recordCount);
        Db2IndexFileFormat.WriteInt32(buf, Db2IndexFileFormat.HdrTreeHeightOffset, treeHeight);
        stream.Position = 0;
        stream.Write(buf, 0, Db2IndexFileFormat.PageSize);
    }

    private static void WriteLeafPage(
        FileStream stream, byte[] buf,
        IReadOnlyList<(ulong EncodedValue, ushort SectionIndex, int RowIndex)> entries,
        int startEntry, int count,
        byte valueByteWidth, long pageOffset, long nextLeafOffset)
    {
        Array.Clear(buf, 0, Db2IndexFileFormat.PageSize);
        buf[Db2IndexFileFormat.LeafTypeOffset] = Db2IndexFileFormat.PageTypeLeaf;
        Db2IndexFileFormat.WriteUInt16(buf, Db2IndexFileFormat.LeafEntryCountOffset, (ushort)count);
        Db2IndexFileFormat.WriteInt64(buf, Db2IndexFileFormat.LeafNextPageOffset, nextLeafOffset);

        var pos = Db2IndexFileFormat.LeafEntriesOffset;
        for (var j = 0; j < count; j++)
        {
            var (encodedValue, sectionIndex, rowIndex) = entries[startEntry + j];
            Db2IndexFileFormat.WriteKey(buf, pos, encodedValue, valueByteWidth);
            pos += valueByteWidth;
            Db2IndexFileFormat.WriteUInt16(buf, pos, sectionIndex);
            pos += 2;
            Db2IndexFileFormat.WriteInt32(buf, pos, rowIndex);
            pos += 4;
        }

        stream.Position = pageOffset;
        stream.Write(buf, 0, Db2IndexFileFormat.PageSize);
    }

    private static void WriteInternalPage(
        FileStream stream, byte[] buf,
        List<(ulong FirstKey, long PageOffset)> levelNodes,
        int chunkStart, int chunkSize,
        byte valueByteWidth, long pageOffset)
    {
        var keyCount = (ushort)(chunkSize - 1);

        Array.Clear(buf, 0, Db2IndexFileFormat.PageSize);
        buf[Db2IndexFileFormat.InternalTypeOffset] = Db2IndexFileFormat.PageTypeInternal;
        Db2IndexFileFormat.WriteUInt16(buf, Db2IndexFileFormat.InternalKeyCountOffset, keyCount);

        var keyPos = Db2IndexFileFormat.InternalKeysOffset;
        for (var i = 1; i <= keyCount; i++)
        {
            Db2IndexFileFormat.WriteKey(buf, keyPos, levelNodes[chunkStart + i].FirstKey, valueByteWidth);
            keyPos += valueByteWidth;
        }

        var childPos = keyPos;
        for (var i = 0; i < chunkSize; i++)
        {
            Db2IndexFileFormat.WriteInt64(buf, childPos, levelNodes[chunkStart + i].PageOffset);
            childPos += 8;
        }

        stream.Position = pageOffset;
        stream.Write(buf, 0, Db2IndexFileFormat.PageSize);
    }
}
