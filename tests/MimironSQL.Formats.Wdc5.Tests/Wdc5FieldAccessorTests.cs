using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

using Shouldly;

using MimironSQL.Db2;
using MimironSQL.Formats.Wdc5.Db2;
using MimironSQL.Providers;

namespace MimironSQL.Formats.Wdc5.Tests;

public sealed class Wdc5FieldAccessorTests(Wdc5TestFixture fixture) : IClassFixture<Wdc5TestFixture>
{
    private readonly Wdc5TestFixture _fixture = fixture;

    [Fact]
    public void CreateFieldAccessor_DenseImmediateScalar_MatchesReadField_AndMaterializesLazyRecords()
    {
        using var stream = _fixture.CreateSingleSectionDenseImmediateScalarFile(rowId: 42, value: 0x01020304);
        var file = new Wdc5File(stream, new Wdc5FileOptions(RecordLoadingMode: Wdc5RecordLoadingMode.Lazy));

        file.ParsedSections[0].RecordsData.ShouldBeNull();
        file.TryGetRowById(42, out var row).ShouldBeTrue();

        var accessor = file.CreateFieldAccessor(sectionIndex: 0, fieldIndex: 0);

        accessor.ReadScalar<int>(row.RowIndexInSection).ShouldBe(file.ReadField<int>(row, 0));
        accessor.ReadScalarRaw(row.RowIndexInSection).ShouldBe(0x01020304u);
        file.ParsedSections[0].RecordsData.ShouldNotBeNull();
    }

    [Fact]
    public void CreateFieldAccessor_SparseFile_Throws()
    {
        using var stream = _fixture.CreateSingleSectionSparseMultiFieldInlineStringFile(rowId: 99, field0Value: 1234, field1StringBytes: [(byte)'h', (byte)'i', 0]);
        var file = new Wdc5File(stream);

        Should.Throw<NotSupportedException>(() => file.CreateFieldAccessor(sectionIndex: 0, fieldIndex: 0))
            .Message.ShouldContain("CreateSparseFieldAccessor");
    }

    [Fact]
    public void CreateSparseFieldAccessor_SparseImmediateScalar_MatchesReadField()
    {
        using var stream = _fixture.CreateSingleSectionSparseMultiFieldInlineStringFile(rowId: 77, field0Value: 0x0A0B0C0D, field1StringBytes: [(byte)'o', (byte)'k', 0]);
        var file = new Wdc5File(stream, new Wdc5FileOptions(RecordLoadingMode: Wdc5RecordLoadingMode.Lazy));
        file.TryGetRowById(77, out var row).ShouldBeTrue();

        var accessor = file.CreateSparseFieldAccessor(sectionIndex: 0, fieldIndex: 0);

        accessor.ReadScalar<int>(row.RowIndexInSection).ShouldBe(file.ReadField<int>(row, 0));
        accessor.ReadScalarRaw(row.RowIndexInSection).ShouldBe(0x0A0B0C0Du);
    }

    [Fact]
    public void CreateFieldAccessor_DenseCommonScalar_MatchesReadField_ForOverrideAndDefault()
    {
        using var stream = CreateDenseCommonFile(id0: 1000, id1: 1001, defaultValue: 42u, commonEntryId: 1000, commonEntryValue: 777u);
        var file = new Wdc5File(stream, new Wdc5FileOptions(RecordLoadingMode: Wdc5RecordLoadingMode.Lazy));

        file.TryGetRowById(1000, out var overrideRow).ShouldBeTrue();
        file.TryGetRowById(1001, out var defaultRow).ShouldBeTrue();

        var accessor = file.CreateFieldAccessor(sectionIndex: 0, fieldIndex: 1);

        accessor.ReadScalar<uint>(overrideRow.RowIndexInSection).ShouldBe(file.ReadField<uint>(overrideRow, 1));
        accessor.ReadScalarRaw(overrideRow.RowIndexInSection).ShouldBe(777u);
        accessor.ReadScalar<uint>(defaultRow.RowIndexInSection).ShouldBe(file.ReadField<uint>(defaultRow, 1));
        accessor.ReadScalarRaw(defaultRow.RowIndexInSection).ShouldBe(42u);
    }

    [Fact]
    public void CreateFieldAccessor_DensePalletScalar_MatchesReadField()
    {
        using var stream = CreateDensePalletFile(id0: 2000, palletIndex0: 2, id1: 2001, palletIndex1: 0, palletData: [10u, 20u, 30u, 40u]);
        var file = new Wdc5File(stream, new Wdc5FileOptions(RecordLoadingMode: Wdc5RecordLoadingMode.Lazy));
        file.TryGetRowById(2000, out var row).ShouldBeTrue();

        var accessor = file.CreateFieldAccessor(sectionIndex: 0, fieldIndex: 1);

        accessor.ReadScalar<uint>(row.RowIndexInSection).ShouldBe(file.ReadField<uint>(row, 1));
        accessor.ReadScalarRaw(row.RowIndexInSection).ShouldBe(30u);
    }

    [Fact]
    public void CreateFieldAccessor_DenseSignedImmediateScalar_MatchesReadField()
    {
        using var stream = CreateSingleSectionDenseSignedImmediateScalarFile(rowId: 88, value: -1, bitWidth: 3);
        var file = new Wdc5File(stream, new Wdc5FileOptions(RecordLoadingMode: Wdc5RecordLoadingMode.Lazy));
        file.TryGetRowById(88, out var row).ShouldBeTrue();

        var accessor = file.CreateFieldAccessor(sectionIndex: 0, fieldIndex: 0);

        accessor.ReadScalar<int>(row.RowIndexInSection).ShouldBe(file.ReadField<int>(row, 0));
        accessor.ReadScalarRaw(row.RowIndexInSection).ShouldBe(unchecked((ulong)-1L));
    }

    [Fact]
    public void CreateFieldAccessor_EncryptedSection_Throws()
    {
        const ulong tactKeyLookup = 0x1122334455667788;
        var keyBytes = Convert.FromHexString("00112233445566778899AABBCCDDEEFF");

        using var stream = _fixture.CreateSingleSectionEncryptedImmediateScalarFile(rowId: 33, tactKeyLookup, keyBytes, value: 1234);
        var file = new Wdc5File(stream, new Wdc5FileOptions(TactKeyProvider: new TestTactKeyProvider(tactKeyLookup, keyBytes)));

        Should.Throw<NotSupportedException>(() => file.CreateFieldAccessor(sectionIndex: 0, fieldIndex: 0))
            .Message.ShouldContain("encrypted sections");
    }

    [Fact]
    public void SparseOffsetTable_IsLazyByDefault_AndBuiltOnFirstSparseStringAccess()
    {
        using var stream = _fixture.CreateSingleSectionSparseScalar16ThenInlineStringFile(rowId: 201, field0: 0xABCD, field1StringBytes: [(byte)'h', (byte)'i', 0]);
        var file = new Wdc5File(stream);

        file.ParsedSections[0].SparseOffsetTable.ShouldNotBeNull();
        file.ParsedSections[0].SparseOffsetTable!.IsValueCreated.ShouldBeFalse();

        file.TryGetRowById(201, out var row).ShouldBeTrue();
        file.ReadField<string>(row, 1).ShouldBe("hi");

        file.ParsedSections[0].SparseOffsetTable!.IsValueCreated.ShouldBeTrue();
    }

    [Fact]
    public void SparseOffsetTable_CanBeBuiltEagerly()
    {
        using var stream = _fixture.CreateSingleSectionSparseScalar16ThenInlineStringFile(rowId: 202, field0: 0x1234, field1StringBytes: [(byte)'y', (byte)'e', (byte)'s', 0]);
        var file = new Wdc5File(stream, new Wdc5FileOptions(EagerSparseOffsetTable: true));

        file.ParsedSections[0].SparseOffsetTable.ShouldNotBeNull();
        file.ParsedSections[0].SparseOffsetTable!.IsValueCreated.ShouldBeTrue();
    }

    private static MemoryStream CreateDenseCommonFile(int id0, int id1, uint defaultValue, int commonEntryId, uint commonEntryValue)
    {
        var method = typeof(Wdc5Tests).GetMethod("CreateSingleSectionDenseTwoFieldCommonFile", BindingFlags.NonPublic | BindingFlags.Static)!;
        return (MemoryStream)method.Invoke(null, [id0, id1, defaultValue, commonEntryId, commonEntryValue])!;
    }

    private static MemoryStream CreateDensePalletFile(int id0, int palletIndex0, int id1, int palletIndex1, uint[] palletData)
    {
        var method = typeof(Wdc5Tests).GetMethod("CreateSingleSectionDenseTwoFieldPalletFile", BindingFlags.NonPublic | BindingFlags.Static)!;
        return (MemoryStream)method.Invoke(null, [id0, palletIndex0, id1, palletIndex1, palletData])!;
    }

    private static MemoryStream CreateSingleSectionDenseSignedImmediateScalarFile(int rowId, int value, int bitWidth)
    {
        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        const int fieldsCount = 1;
        const int recordsCount = 1;
        const int recordSize = 8;
        const int sectionsCount = 1;
        const int sectionFileOffset = 512;

        WriteWdc5Header(
            writer,
            recordsCount: recordsCount,
            fieldsCount: fieldsCount,
            recordSize: recordSize,
            stringTableSize: 0,
            minIndex: rowId,
            maxIndex: rowId,
            flags: 0,
            idFieldIndex: 0,
            sectionsCount: sectionsCount);

        WriteWdc5SectionHeader(writer, new Wdc5SectionHeader(
            TactKeyLookup: 0,
            FileOffset: sectionFileOffset,
            NumRecords: recordsCount,
            StringTableSize: 0,
            OffsetRecordsEndOffset: 0,
            IndexDataSize: 4,
            ParentLookupDataSize: 0,
            OffsetMapIDCount: 0,
            CopyTableCount: 0));

        writer.Write((short)0);
        writer.Write((short)0);

        var columnMeta = new ColumnMetaData
        {
            RecordOffset = 0,
            Size = 32,
            AdditionalDataSize = 0,
            CompressionType = CompressionType.SignedImmediate,
            Immediate = new ColumnCompressionDataImmediate(BitOffset: 0, BitWidth: bitWidth, Flags: 0),
        };
        WriteStruct(writer, columnMeta);

        PadTo(writer, sectionFileOffset);

        writer.Write(EncodeSignedBits(value, bitWidth));
        writer.Write(0);
        writer.Write(0);

        ms.Position = 0;
        return ms;
    }

    private static uint EncodeSignedBits(int value, int bitWidth)
    {
        var mask = (1u << bitWidth) - 1u;
        return unchecked((uint)value) & mask;
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

    private sealed class TestTactKeyProvider(ulong tactKeyLookup, ReadOnlyMemory<byte> keyBytes) : ITactKeyProvider
    {
        public bool TryGetKey(ulong lookup, out ReadOnlyMemory<byte> key)
        {
            if (lookup == tactKeyLookup)
            {
                key = keyBytes;
                return true;
            }

            key = default;
            return false;
        }
    }
}
