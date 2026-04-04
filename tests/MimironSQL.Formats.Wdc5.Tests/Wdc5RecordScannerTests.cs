using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

using MimironSQL.Db2;
using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5.Db2;

using Shouldly;

namespace MimironSQL.Formats.Wdc5.Tests;

public sealed class Wdc5RecordScannerTests(Wdc5TestFixture fixture) : IClassFixture<Wdc5TestFixture>
{
    private readonly Wdc5TestFixture _fixture = fixture;

    [Fact]
    public void ScanFieldEquals_DenseImmediateScalar_MatchesBaseline()
    {
        using var stream = CreateDenseImmediateTargetFile([(100, 11), (101, 22), (102, 11)]);
        using var file = new Wdc5File(stream);

        AssertMatchesBaseline(file, fieldIndex: 1, value: 11);
    }

    [Fact]
    public void ScanFieldEquals_DenseImmediateScalar_FalsePositiveInOtherColumn_IsRejected()
    {
        using var stream = CreateDenseImmediateTargetFile([(100, 11), (101, 22)], dummyField0Value: 777);
        using var file = new Wdc5File(stream);

        Scan(file, fieldIndex: 1, value: 777).ShouldBeEmpty();
    }

    [Fact]
    public void ScanFieldEquals_DenseShortImmediate_UsesStridedFallbackAndMatchesBaseline()
    {
        using var stream = CreateDenseImmediateTargetFile([(100, 7), (101, 9), (102, 7)], bitWidth: 16);
        using var file = new Wdc5File(stream);

        AssertMatchesBaseline(file, fieldIndex: 1, value: 7);
    }

    [Fact]
    public void ScanFieldEquals_VirtualId_MatchesBaseline()
    {
        using var stream = CreateDenseImmediateTargetFile([(500, 11), (501, 22), (502, 33)]);
        using var file = new Wdc5File(stream);

        AssertMatchesBaseline(file, fieldIndex: Db2VirtualFieldIndex.Id, value: 501);
    }

    [Fact]
    public void ScanFieldEquals_CommonCompression_MatchesBaselineForDefaultAndOverride()
    {
        using var stream = CreateCommonTargetFile(id0: 1000, id1: 1001, defaultValue: 42, commonEntryId: 1000, commonEntryValue: 777);
        using var file = new Wdc5File(stream);

        AssertMatchesBaseline(file, fieldIndex: 1, value: 42u);
        AssertMatchesBaseline(file, fieldIndex: 1, value: 777u);
    }

    [Fact]
    public void ScanFieldEquals_PalletCompression_MatchesBaseline()
    {
        using var stream = CreatePalletTargetFile(id0: 2000, palletIndex0: 2, id1: 2001, palletIndex1: 0, palletData: [10u, 20u, 30u, 40u]);
        using var file = new Wdc5File(stream);

        AssertMatchesBaseline(file, fieldIndex: 1, value: 30u);
    }

    [Fact]
    public void ScanFieldEquals_PalletCompression_WithDuplicatePalletValues_MatchesBaseline()
    {
        using var stream = CreatePalletTargetFile(id0: 2100, palletIndex0: 0, id1: 2101, palletIndex1: 1, palletData: [20u, 20u, 30u, 40u]);
        using var file = new Wdc5File(stream);

        AssertMatchesBaseline(file, fieldIndex: 1, value: 20u);
    }

    [Fact]
    public void ScanFieldEquals_PalletArray_MatchesAnyElement()
    {
        using var stream = CreatePalletArrayTargetFile(rowId: 3000, palletArrayIndex: 1, cardinality: 3, palletData: [1u, 2u, 3u, 10u, 20u, 30u]);
        using var file = new Wdc5File(stream);

        AssertMatchesBaseline(file, fieldIndex: 1, value: 20u);
    }

    [Fact]
    public void ScanFieldEquals_SparseImmediateField_MatchesBaseline()
    {
        using var stream = _fixture.CreateSingleSectionSparseMultiFieldInlineStringFile(rowId: 99, field0Value: 0x0A0B0C0D, field1StringBytes: [(byte)'h', (byte)'i', 0]);
        using var file = new Wdc5File(stream);

        AssertMatchesBaseline(file, fieldIndex: 0, value: 0x0A0B0C0D);
    }

    [Fact]
    public void ScanFieldEquals_MultiSection_MatchesBaseline()
    {
        using var stream = CreateTwoSectionDenseImmediateTargetFile((100, 11), (101, 22));
        using var file = new Wdc5File(stream);

        AssertMatchesBaseline(file, fieldIndex: 1, value: 22);
    }

    private static void AssertMatchesBaseline<T>(Wdc5File file, int fieldIndex, T value) where T : unmanaged
    {
        var actual = Scan(file, fieldIndex, value)
            .Select(h => (h.SectionIndex, h.RowIndexInSection, h.RowId))
            .OrderBy(x => x.SectionIndex)
            .ThenBy(x => x.RowIndexInSection)
            .ToArray();

        var expected = EnumerateBaselineMatches(file, fieldIndex, value)
            .Select(h => (h.SectionIndex, h.RowIndexInSection, h.RowId))
            .OrderBy(x => x.SectionIndex)
            .ThenBy(x => x.RowIndexInSection)
            .ToArray();

        actual.ShouldBe(expected);
    }

    private static List<RowHandle> Scan<T>(Wdc5File file, int fieldIndex, T value) where T : unmanaged
    {
        var scanner = new Wdc5RecordScanner(file);
        var results = new List<RowHandle>();
        scanner.ScanFieldEquals(fieldIndex, value, results);
        return results;
    }

    private static IEnumerable<RowHandle> EnumerateBaselineMatches<T>(Wdc5File file, int fieldIndex, T value) where T : unmanaged
    {
        if (fieldIndex == Db2VirtualFieldIndex.Id)
        {
            foreach (var handle in file.EnumerateRowHandles())
            {
                if (EqualityComparer<T>.Default.Equals(file.ReadField<T>(handle, fieldIndex), value))
                {
                    yield return handle;
                }
            }

            yield break;
        }

        var fieldMeta = file.FieldMeta[fieldIndex];
        var columnMeta = file.ColumnMeta[fieldIndex];
        var fieldBitWidth = GetFieldBitWidth(fieldMeta, columnMeta);
        var isArrayField = columnMeta.CompressionType == CompressionType.PalletArray && columnMeta.Pallet.Cardinality != 1
            || (columnMeta.CompressionType == CompressionType.None && columnMeta.Size > fieldBitWidth);

        foreach (var handle in file.EnumerateRowHandles())
        {
            if (!isArrayField)
            {
                if (EqualityComparer<T>.Default.Equals(file.ReadField<T>(handle, fieldIndex), value))
                {
                    yield return handle;
                }

                continue;
            }

            var values = file.ReadField<T[]>(handle, fieldIndex);
            if (values.Contains(value))
            {
                yield return handle;
            }
        }
    }

    private static int GetFieldBitWidth(FieldMetaData fieldMeta, ColumnMetaData columnMeta)
        => columnMeta.CompressionType switch
        {
            CompressionType.None => Math.Max(32 - fieldMeta.Bits, columnMeta.Immediate.BitWidth),
            CompressionType.Immediate or CompressionType.SignedImmediate => columnMeta.Immediate.BitWidth,
            CompressionType.Pallet or CompressionType.PalletArray => columnMeta.Pallet.BitWidth,
            CompressionType.Common => 32,
            _ => throw new NotSupportedException($"Unexpected compression type {columnMeta.CompressionType}.")
        };

    private static MemoryStream CreateDenseImmediateTargetFile((int RowId, int Value)[] rows, int dummyField0Value = 0, int bitWidth = 32)
    {
        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        const int fieldsCount = 2;
        const int sectionsCount = 1;
        const int recordSize = 8;
        const int sectionFileOffset = 512;

        WriteWdc5Header(
            writer,
            recordsCount: rows.Length,
            fieldsCount: fieldsCount,
            recordSize: recordSize,
            stringTableSize: 0,
            minIndex: rows.Min(x => x.RowId),
            maxIndex: rows.Max(x => x.RowId),
            flags: 0,
            idFieldIndex: 0,
            sectionsCount: sectionsCount);

        WriteWdc5SectionHeader(writer, new Wdc5SectionHeader(
            TactKeyLookup: 0,
            FileOffset: sectionFileOffset,
            NumRecords: rows.Length,
            StringTableSize: 0,
            OffsetRecordsEndOffset: 0,
            IndexDataSize: rows.Length * 4,
            ParentLookupDataSize: 0,
            OffsetMapIDCount: 0,
            CopyTableCount: 0));

        writer.Write((short)1);
        writer.Write((short)0);
        writer.Write((short)(32 - bitWidth));
        writer.Write((short)0);

        var field0 = new ColumnMetaData
        {
            RecordOffset = 0,
            Size = 32,
            AdditionalDataSize = 0,
            CompressionType = CompressionType.Immediate,
            Immediate = new ColumnCompressionDataImmediate(BitOffset: 0, BitWidth: 32, Flags: 0),
        };

        var field1 = new ColumnMetaData
        {
            RecordOffset = 32,
            Size = checked((ushort)bitWidth),
            AdditionalDataSize = 0,
            CompressionType = CompressionType.Immediate,
            Immediate = new ColumnCompressionDataImmediate(BitOffset: 0, BitWidth: bitWidth, Flags: 0),
        };

        WriteStruct(writer, field0);
        WriteStruct(writer, field1);

        PadTo(writer, sectionFileOffset);

        foreach (var row in rows)
        {
            writer.Write(dummyField0Value);

            if (bitWidth <= 16)
            {
                writer.Write((ushort)row.Value);
                writer.Write((ushort)0);
            }
            else
            {
                writer.Write(row.Value);
            }
        }

        foreach (var row in rows)
        {
            writer.Write(row.RowId);
        }

        ms.Position = 0;
        return ms;
    }

    private static MemoryStream CreateTwoSectionDenseImmediateTargetFile((int RowId, int Value) section0Row, (int RowId, int Value) section1Row)
    {
        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        const int fieldsCount = 2;
        const int sectionsCount = 2;
        const int recordSize = 8;
        const int section0FileOffset = 512;
        const int section1FileOffset = 768;

        WriteWdc5Header(
            writer,
            recordsCount: 2,
            fieldsCount: fieldsCount,
            recordSize: recordSize,
            stringTableSize: 0,
            minIndex: Math.Min(section0Row.RowId, section1Row.RowId),
            maxIndex: Math.Max(section0Row.RowId, section1Row.RowId),
            flags: 0,
            idFieldIndex: 0,
            sectionsCount: sectionsCount);

        WriteWdc5SectionHeader(writer, new Wdc5SectionHeader(
            TactKeyLookup: 0,
            FileOffset: section0FileOffset,
            NumRecords: 1,
            StringTableSize: 0,
            OffsetRecordsEndOffset: 0,
            IndexDataSize: 4,
            ParentLookupDataSize: 0,
            OffsetMapIDCount: 0,
            CopyTableCount: 0));

        WriteWdc5SectionHeader(writer, new Wdc5SectionHeader(
            TactKeyLookup: 0,
            FileOffset: section1FileOffset,
            NumRecords: 1,
            StringTableSize: 0,
            OffsetRecordsEndOffset: 0,
            IndexDataSize: 4,
            ParentLookupDataSize: 0,
            OffsetMapIDCount: 0,
            CopyTableCount: 0));

        writer.Write((short)1);
        writer.Write((short)0);
        writer.Write((short)1);
        writer.Write((short)0);

        var field0 = new ColumnMetaData
        {
            RecordOffset = 0,
            Size = 32,
            AdditionalDataSize = 0,
            CompressionType = CompressionType.Immediate,
            Immediate = new ColumnCompressionDataImmediate(BitOffset: 0, BitWidth: 32, Flags: 0),
        };

        var field1 = new ColumnMetaData
        {
            RecordOffset = 32,
            Size = 32,
            AdditionalDataSize = 0,
            CompressionType = CompressionType.Immediate,
            Immediate = new ColumnCompressionDataImmediate(BitOffset: 0, BitWidth: 32, Flags: 0),
        };

        WriteStruct(writer, field0);
        WriteStruct(writer, field1);

        PadTo(writer, section0FileOffset);
        writer.Write(0);
        writer.Write(section0Row.Value);
        writer.Write(section0Row.RowId);

        PadTo(writer, section1FileOffset);
        writer.Write(0);
        writer.Write(section1Row.Value);
        writer.Write(section1Row.RowId);

        ms.Position = 0;
        return ms;
    }

    private static MemoryStream CreateCommonTargetFile(int id0, int id1, uint defaultValue, int commonEntryId, uint commonEntryValue)
    {
        var method = typeof(Wdc5Tests).GetMethod("CreateSingleSectionDenseTwoFieldCommonFile", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        return (MemoryStream)method.Invoke(null, [id0, id1, defaultValue, commonEntryId, commonEntryValue])!;
    }

    private static MemoryStream CreatePalletTargetFile(int id0, int palletIndex0, int id1, int palletIndex1, uint[] palletData)
    {
        var method = typeof(Wdc5Tests).GetMethod("CreateSingleSectionDenseTwoFieldPalletFile", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        return (MemoryStream)method.Invoke(null, [id0, palletIndex0, id1, palletIndex1, palletData])!;
    }

    private static MemoryStream CreatePalletArrayTargetFile(int rowId, int palletArrayIndex, int cardinality, uint[] palletData)
    {
        var method = typeof(Wdc5Tests).GetMethod("CreateSingleSectionDenseTwoFieldPalletArrayFile", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        return (MemoryStream)method.Invoke(null, [rowId, palletArrayIndex, cardinality, palletData])!;
    }

    private static void WriteWdc5Header(
        BinaryWriter writer,
        int recordsCount,
        int fieldsCount,
        int recordSize,
        int stringTableSize,
        int minIndex,
        int maxIndex,
        Db2Flags flags,
        ushort idFieldIndex,
        int sectionsCount)
    {
        writer.Write(0x35434457u);
        writer.Write(1u);
        writer.Write(new byte[128]);
        writer.Write(recordsCount);
        writer.Write(fieldsCount);
        writer.Write(recordSize);
        writer.Write(stringTableSize);
        writer.Write(0u);
        writer.Write(0x12345678u);
        writer.Write(minIndex);
        writer.Write(maxIndex);
        writer.Write(0);
        writer.Write((ushort)flags);
        writer.Write(idFieldIndex);
        writer.Write(0);
        writer.Write(0);
        writer.Write(0);
        writer.Write(0);
        writer.Write(0);
        writer.Write(0);
        writer.Write(sectionsCount);
    }

    private static void WriteWdc5SectionHeader(BinaryWriter writer, Wdc5SectionHeader header)
        => WriteStruct(writer, header);

    private static void WriteStruct<T>(BinaryWriter writer, T value) where T : unmanaged
    {
        Span<byte> bytes = stackalloc byte[Unsafe.SizeOf<T>()];
        MemoryMarshal.Write(bytes, in value);
        writer.Write(bytes);
    }

    private static void PadTo(BinaryWriter writer, int position)
    {
        while (writer.BaseStream.Position < position)
        {
            writer.Write((byte)0);
        }
    }
}