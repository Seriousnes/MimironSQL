using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Reflection;
using System.Text;

using MimironSQL.Db2;
using MimironSQL.Providers;

using NSubstitute;

using Security.Cryptography;

using Shouldly;

namespace MimironSQL.Formats.Wdc5.Tests;

public sealed class Wdc5Tests : IClassFixture<Wdc5TestFixture>
{
    private readonly Wdc5TestFixture _fixture;

    public Wdc5Tests(Wdc5TestFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public void Wdc5File_Ctor_MinimalHeader_ParsesWithZeroFieldsAndSections()
    {
        using var stream = CreateMinimalWdc5Stream(layoutHash: 0x12345678, fieldsCount: 0, recordsCount: 0, sectionsCount: 0);
        var file = new Wdc5File(stream);
        Assert.Equal(0x12345678u, file.Header.LayoutHash);
        Assert.Equal(0, file.Header.FieldsCount);
        Assert.Empty(file.Sections);
        Assert.Empty(file.ParsedSections);
        Assert.Equal(0, file.RecordsCount);
    }

    [Fact]
    public void Wdc5Format_GetLayout_UsesHeaderHashAndFieldsCount()
    {
        using var stream = CreateMinimalWdc5Stream(layoutHash: 0xCAFEBABE, fieldsCount: 7, recordsCount: 0, sectionsCount: 0);
        var format = Wdc5Format.Instance;
        var file = (Wdc5File)format.OpenFile(stream);
        var layout = format.GetLayout(file);
        Assert.Equal(0xCAFEBABEu, layout.LayoutHash);
        Assert.Equal(7, layout.PhysicalFieldsCount);
    }

    [Fact]
    public void Wdc5File_Ctor_NonSeekableStream_Throws()
    {
        using var stream = new NonSeekableStream(new MemoryStream([1, 2, 3]));
        Should.Throw<NotSupportedException>(() => new Wdc5File(stream));
    }

    [Fact]
    public void Wdc5File_Ctor_WrongMagic_ThrowsWithDetectedFormat()
    {
        using var stream = CreateMinimalWdc5Stream(layoutHash: 0, fieldsCount: 0, recordsCount: 0, sectionsCount: 0, magic: 0x33434457u);
        var ex = Should.Throw<InvalidDataException>(() => new Wdc5File(stream));
        Assert.Contains("Wdc3", ex.Message);
    }

    [Fact]
    public void Wdc5FileLookupTracker_StartAndSnapshot_TracksCallsAndResetsOnDispose()
    {
        Wdc5FileLookupTracker.Snapshot().TotalTryGetRowByIdCalls.ShouldBe(0);

        using (var scope = Wdc5FileLookupTracker.Start())
        {
            Wdc5FileLookupTracker.Snapshot().TotalTryGetRowByIdCalls.ShouldBe(0);

            Wdc5FileLookupTracker.OnTryGetRowById();
            Wdc5FileLookupTracker.OnTryGetRowById();

            Wdc5FileLookupTracker.Snapshot().TotalTryGetRowByIdCalls.ShouldBe(2);
        }

        Wdc5FileLookupTracker.Snapshot().TotalTryGetRowByIdCalls.ShouldBe(0);
    }

    [Fact]
    public void Wdc5Section_BuildSparseRecordStartBits_EmptyEntries_ReturnsEmpty()
    {
        Wdc5Section.BuildSparseRecordStartBits([], sectionFileOffset: 0, recordDataSizeBytes: 123).ShouldBeEmpty();
    }

    [Fact]
    public void Wdc5Section_BuildSparseRecordStartBits_AllZeroOffsets_UsesSizesAsContiguous()
    {
        var entries = new SparseEntry[]
        {
            new(Offset: 0, Size: 2),
            new(Offset: 0, Size: 4),
        };

        Wdc5Section.BuildSparseRecordStartBits(entries, sectionFileOffset: 1000, recordDataSizeBytes: 6)
            .ShouldBe([0, 16]);
    }

    [Fact]
    public void Wdc5Section_BuildSparseRecordStartBits_ExplicitOffsets_AreValidatedAndConvertedToBits()
    {
        var entries = new SparseEntry[]
        {
            new(Offset: 1010, Size: 4),
            new(Offset: 1014, Size: 2),
        };

        Wdc5Section.BuildSparseRecordStartBits(entries, sectionFileOffset: 1000, recordDataSizeBytes: 16)
            .ShouldBe([80, 112]);
    }

    [Fact]
    public void Wdc5Section_BuildSparseRecordStartBits_ThrowsOnUnsortedOffsets()
    {
        var entries = new SparseEntry[]
        {
            new(Offset: 1014, Size: 2),
            new(Offset: 1010, Size: 4),
        };

        Should.Throw<InvalidDataException>(() => Wdc5Section.BuildSparseRecordStartBits(entries, sectionFileOffset: 1000, recordDataSizeBytes: 16))
            .Message.ShouldContain("not sorted");
    }

    [Fact]
    public void Wdc5File_Ctor_WithSectionAndIndexData_UsesMinIndexWhenIndexDataAllZeros_AndCopyMapResolves()
    {
        using var stream = CreateSingleSectionDenseStringFile(
            flags: 0,
            minIndex: 100,
            idFieldIndex: 0,
            recordValue: 5,
            stringTableBytes: [0, (byte)'h', (byte)'i', 0],
            indexData: [0],
            copyTable: [(DestinationId: 101, SourceId: 100)]);

        var file = new Wdc5File(stream);
        file.ParsedSections.Count.ShouldBe(1);
        file.RecordsCount.ShouldBe(1);
        file.TotalSectionRecordCount.ShouldBe(1);

        file.TryGetRowById(100, out var row100).ShouldBeTrue();
        row100.RowId.ShouldBe(100);

        file.TryGetRowById(101, out var row101).ShouldBeTrue();
        row101.RowId.ShouldBe(101);
        row101.SectionIndex.ShouldBe(row100.SectionIndex);
        row101.RowIndexInSection.ShouldBe(row100.RowIndexInSection);

        file.ReadField<string>(row100, 0).ShouldBe("hi");
        file.TryGetDenseStringTableIndex(row100, fieldIndex: 0, out var stringIndex).ShouldBeTrue();
        stringIndex.ShouldBe(1);
    }

    [Fact]
    public void Wdc5File_Ctor_TwoDenseSections_ConcatsStringTables_AndReadsRows()
    {
        using var stream = CreateTwoSectionDenseStringFile(
            id0: 100,
            stringTable0: [0, (byte)'h', (byte)'i', 0],
            id1: 101,
            stringTable1: [0, (byte)'b', (byte)'y', 0]);

        var file = new Wdc5File(stream);
        file.ParsedSections.Count.ShouldBe(2);
        file.RecordsCount.ShouldBe(2);
        file.TotalSectionRecordCount.ShouldBe(2);

        file.TryGetRowById(100, out var row0).ShouldBeTrue();
        file.ReadField<string>(row0, 0).ShouldBe("hi");
        file.TryGetDenseStringTableIndex(row0, fieldIndex: 0, out var idx0).ShouldBeTrue();
        idx0.ShouldBe(1);

        file.TryGetRowById(101, out var row1).ShouldBeTrue();
        file.ReadField<string>(row1, 0).ShouldBe("by");
        file.TryGetDenseStringTableIndex(row1, fieldIndex: 0, out var idx1).ShouldBeTrue();
        idx1.ShouldBe(5);

        file.EnumerateRowHandles().Select(x => x.RowId).OrderBy(x => x).ShouldBe([100, 101]);
    }

    [Fact]
    public void Wdc5File_TryGetDenseStringTableIndex_OffsetZero_ReturnsSectionBaseOffset()
    {
        using var stream = CreateTwoSectionDenseStringFile(
            id0: 100,
            stringTable0: [0, (byte)'h', (byte)'i', 0],
            id1: 101,
            stringTable1: [0, (byte)'b', (byte)'y', 0],
            recordValueOverride: 0);

        var file = new Wdc5File(stream);

        file.TryGetRowById(100, out var row0).ShouldBeTrue();
        file.ReadField<string>(row0, 0).ShouldBe(string.Empty);
        file.TryGetDenseStringTableIndex(row0, fieldIndex: 0, out var idx0).ShouldBeTrue();
        idx0.ShouldBe(0);

        file.TryGetRowById(101, out var row1).ShouldBeTrue();
        file.ReadField<string>(row1, 0).ShouldBe(string.Empty);
        file.TryGetDenseStringTableIndex(row1, fieldIndex: 0, out var idx1).ShouldBeTrue();
        idx1.ShouldBe(4);
    }

    [Fact]
    public void Wdc5File_ReadField_VirtualParentRelation_UsesParentLookup_WithSecondaryKey()
    {
        using var stream = CreateSingleSectionDenseScalarFileWithParentLookup(
            flags: Db2Flags.SecondaryKey,
            rowId: 123,
            parentRelationId: 777,
            recordValue: 555);

        var file = new Wdc5File(stream);
        file.TryGetRowById(123, out var row).ShouldBeTrue();

        file.ReadField<int>(row, Db2VirtualFieldIndex.ParentRelation).ShouldBe(777);

        object[] values = new object[file.Header.FieldsCount + 2];
        file.ReadAllFields(row, values);
        values[0].ShouldBe(123);
        values[1].ShouldBe(777);
        values[2].ShouldBe(555);
    }

    [Fact]
    public void Wdc5File_ReadField_CommonCompression_UsesPerIdValueOrDefault()
    {
        using var stream = CreateSingleSectionDenseTwoFieldCommonFile(
            id0: 1000,
            id1: 1001,
            defaultValue: 42,
            commonEntryId: 1000,
            commonEntryValue: 777);

        var file = new Wdc5File(stream);

        file.TryGetRowById(1000, out var row0).ShouldBeTrue();
        file.ReadField<uint>(row0, 1).ShouldBe(777u);

        file.TryGetRowById(1001, out var row1).ShouldBeTrue();
        file.ReadField<uint>(row1, 1).ShouldBe(42u);
    }

    [Fact]
    public void Wdc5File_ReadField_PalletCompression_ReadsFromPalletData()
    {
        using var stream = CreateSingleSectionDenseTwoFieldPalletFile(
            id0: 2000,
            palletIndex0: 2,
            id1: 2001,
            palletIndex1: 0,
            palletData: [10u, 20u, 30u, 40u]);

        var file = new Wdc5File(stream);

        file.TryGetRowById(2000, out var row0).ShouldBeTrue();
        file.ReadField<uint>(row0, 1).ShouldBe(30u);

        file.TryGetRowById(2001, out var row1).ShouldBeTrue();
        file.ReadField<uint>(row1, 1).ShouldBe(10u);
    }

    [Fact]
    public void Wdc5File_ReadField_PalletArray_ReadsArrayFromPalletData()
    {
        using var stream = CreateSingleSectionDenseTwoFieldPalletArrayFile(
            rowId: 3000,
            palletArrayIndex: 1,
            cardinality: 3,
            palletData: [1u, 2u, 3u, 10u, 20u, 30u]);

        var file = new Wdc5File(stream);
        file.TryGetRowById(3000, out var row).ShouldBeTrue();
        file.ReadField<int[]>(row, 1).ShouldBe([10, 20, 30]);
    }

    [Fact]
    public void Wdc5File_SparseOffsetMap_TwoRows_ReadsInlineStrings()
    {
        using var stream = CreateSingleSectionSparseOffsetMapInlineStringFile(
            row0Id: 4000,
            row0Bytes: [(byte)'a', (byte)'b', (byte)'c', 0],
            row1Id: 4001,
            row1Bytes: [(byte)'x', (byte)'y', (byte)'z', (byte)'!', 0]);

        var file = new Wdc5File(stream);

        file.TryGetRowById(4000, out var row0).ShouldBeTrue();
        file.ReadField<string>(row0, 0).ShouldBe("abc");

        file.TryGetRowById(4001, out var row1).ShouldBeTrue();
        file.ReadField<string>(row1, 0).ShouldBe("xyz!");

        file.EnumerateRowHandles().Select(x => x.RowId).OrderBy(x => x).ShouldBe([4000, 4001]);
    }

    [Fact]
    public void Wdc5File_ReadField_String_InvalidUtf8_ReturnsEmptyString()
    {
        using var stream = CreateSingleSectionDenseStringFile(
            flags: 0,
            minIndex: 100,
            idFieldIndex: 0,
            recordValue: 5,
            stringTableBytes: [0, 0xC3, 0x28, 0],
            indexData: [0],
            copyTable: []);

        var file = new Wdc5File(stream);
        file.TryGetRowById(100, out var row).ShouldBeTrue();
        file.ReadField<string>(row, 0).ShouldBe(string.Empty);
    }

    [Fact]
    public void Wdc5File_ReadField_SparseInlineString_ReadsFromRecordBytes()
    {
        using var stream = CreateSingleSectionSparseInlineStringFile(
            sparseRowId: 200,
            rowBytes: [(byte)'a', (byte)'b', (byte)'c', 0]);

        var file = new Wdc5File(stream);
        file.TryGetRowById(200, out var row).ShouldBeTrue();
        file.ReadField<string>(row, 0).ShouldBe("abc");
    }

    [Fact]
    public void Wdc5File_ReadAllFields_IncludesVirtualIdAndStringField()
    {
        using var stream = CreateSingleSectionDenseStringFile(
            flags: 0,
            minIndex: 100,
            idFieldIndex: 0,
            recordValue: 5,
            stringTableBytes: [0, (byte)'h', (byte)'i', 0],
            indexData: [0],
            copyTable: []);

        var file = new Wdc5File(stream);
        file.TryGetRowById(100, out var row).ShouldBeTrue();

        object[] values = new object[file.Header.FieldsCount + 2];
        file.ReadAllFields(row, values);

        values[0].ShouldBe(100);
        values[1].ShouldBe(0);
        values[2].ShouldBe("hi");
    }

    [Fact]
    public void Wdc5File_Ctor_NoIndexData_DecodesIdFromRecordBits()
    {
        using var stream = CreateSingleSectionDenseScalarFile(
            flags: 0,
            idFieldIndex: 0,
            recordValue: 555);

        var file = new Wdc5File(stream);
        file.TryGetRowById(555, out var row).ShouldBeTrue();
        file.ReadField<int>(row, Db2VirtualFieldIndex.Id).ShouldBe(555);
    }

    [Fact]
    public void Wdc5File_TryGetRowHandle_SupportsMultipleIdTypes_AndThrowsOnTooLargeUlong()
    {
        using var stream = CreateSingleSectionDenseScalarFile(flags: 0, idFieldIndex: 0, recordValue: 555);
        var file = new Wdc5File(stream);

        file.TryGetRowHandle(555u, out _).ShouldBeTrue();
        file.TryGetRowHandle((short)555, out _).ShouldBeTrue();
        file.TryGetRowHandle((long)555, out _).ShouldBeTrue();

        Should.Throw<OverflowException>(() => file.TryGetRowHandle(ulong.MaxValue, out _));
    }

    [Fact]
    public void Wdc5File_TryGetDenseStringTableIndex_NegativeFieldIndex_ReturnsFalse()
    {
        using var stream = CreateSingleSectionDenseStringFile(
            flags: 0,
            minIndex: 100,
            idFieldIndex: 0,
            recordValue: 5,
            stringTableBytes: [0, (byte)'h', (byte)'i', 0],
            indexData: [0],
            copyTable: []);

        var file = new Wdc5File(stream);
        file.TryGetRowById(100, out var row).ShouldBeTrue();
        file.TryGetDenseStringTableIndex(row, fieldIndex: -1, out _).ShouldBeFalse();
    }

    [Fact]
    public void Wdc5File_TryGetDenseStringTableIndex_SparseFile_ReturnsFalse()
    {
        using var stream = CreateSingleSectionSparseInlineStringFile(
            sparseRowId: 200,
            rowBytes: [(byte)'a', (byte)'b', (byte)'c', 0]);

        var file = new Wdc5File(stream);
        file.TryGetRowById(200, out var row).ShouldBeTrue();
        file.TryGetDenseStringTableIndex(row, fieldIndex: 0, out _).ShouldBeFalse();
    }

    [Fact]
    public void Wdc5File_ReadField_EncryptedSection_DecryptsImmediateScalar()
    {
        const int rowId = 120;
        const ulong tactKeyLookup = 0x1122334455667788;

        var keyBytes = (ReadOnlyMemory<byte>)new byte[]
        {
            0x00, 0x11, 0x22, 0x33,
            0x44, 0x55, 0x66, 0x77,
            0x88, 0x99, 0xAA, 0xBB,
            0xCC, 0xDD, 0xEE, 0xFF,
        };

        var keyProvider = Substitute.For<ITactKeyProvider>();
        keyProvider
            .TryGetKey(tactKeyLookup, out Arg.Any<ReadOnlyMemory<byte>>())
            .Returns(callInfo =>
            {
                callInfo[1] = keyBytes;
                return true;
            });

        using var stream = _fixture.CreateSingleSectionEncryptedImmediateScalarFile(rowId, tactKeyLookup, keyBytes, value: 0x12345678);
        var file = new Wdc5File(stream, new Wdc5FileOptions(TactKeyProvider: keyProvider));

        file.TryGetRowById(rowId, out var row).ShouldBeTrue();
        file.ReadField<int>(row, 0).ShouldBe(0x12345678);

        object[] values = new object[file.Header.FieldsCount + 2];
        file.ReadAllFields(row, values);
        values[0].ShouldBe(rowId);
        values[1].ShouldBe(0);
        values[2].ShouldBe(0x12345678);
    }

    [Fact]
    public void Wdc5File_ReadFieldBoxed_EncryptedSection_Decrypts()
    {
        const int rowId = 121;
        const ulong tactKeyLookup = 0x9988776655443322;

        var keyBytes = (ReadOnlyMemory<byte>)new byte[]
        {
            0x10, 0x21, 0x32, 0x43,
            0x54, 0x65, 0x76, 0x87,
            0x98, 0xA9, 0xBA, 0xCB,
            0xDC, 0xED, 0xFE, 0x0F,
        };

        var keyProvider = Substitute.For<ITactKeyProvider>();
        keyProvider
            .TryGetKey(tactKeyLookup, out Arg.Any<ReadOnlyMemory<byte>>())
            .Returns(callInfo =>
            {
                callInfo[1] = keyBytes;
                return true;
            });

        using var stream = _fixture.CreateSingleSectionEncryptedImmediateScalarFile(rowId, tactKeyLookup, keyBytes, value: 1234);
        var file = new Wdc5File(stream, new Wdc5FileOptions(TactKeyProvider: keyProvider));
        file.TryGetRowById(rowId, out var row).ShouldBeTrue();

        var method = typeof(Wdc5File).GetMethod("ReadFieldBoxed", BindingFlags.Instance | BindingFlags.NonPublic);
        method.ShouldNotBeNull();

        var boxed = method!.Invoke(file, [row, typeof(int), 0]);
        boxed.ShouldBeOfType<int>();
        boxed.ShouldBe(1234);
    }

    [Fact]
    public void Wdc5File_ReadField_VirtualId_CastsToAllIntegerTypes_AndDecimal()
    {
        using var stream = CreateSingleSectionDenseScalarFile(flags: 0, idFieldIndex: 0, recordValue: 120);
        var file = new Wdc5File(stream);
        file.TryGetRowById(120, out var row).ShouldBeTrue();

        file.ReadField<uint>(row, Db2VirtualFieldIndex.Id).ShouldBe(120u);
        file.ReadField<long>(row, Db2VirtualFieldIndex.Id).ShouldBe(120L);
        file.ReadField<ulong>(row, Db2VirtualFieldIndex.Id).ShouldBe(120UL);
        file.ReadField<short>(row, Db2VirtualFieldIndex.Id).ShouldBe((short)120);
        file.ReadField<ushort>(row, Db2VirtualFieldIndex.Id).ShouldBe((ushort)120);
        file.ReadField<byte>(row, Db2VirtualFieldIndex.Id).ShouldBe((byte)120);
        file.ReadField<sbyte>(row, Db2VirtualFieldIndex.Id).ShouldBe((sbyte)120);
        file.ReadField<decimal>(row, Db2VirtualFieldIndex.Id).ShouldBe(120m);
    }

    [Fact]
    public void Wdc5File_ReadField_UnsupportedVirtualFieldIndex_Throws()
    {
        using var stream = CreateSingleSectionDenseScalarFile(flags: 0, idFieldIndex: 0, recordValue: 555);
        var file = new Wdc5File(stream);
        file.TryGetRowById(555, out var row).ShouldBeTrue();

        Should.Throw<NotSupportedException>(() => file.ReadField<int>(row, Db2VirtualFieldIndex.UnsupportedNonInline));
    }

    [Fact]
    public void Wdc5File_ReadField_EnumType_ReadsUnderlyingValue()
    {
        using var stream = _fixture.CreateSingleSectionDenseImmediateScalarFile(rowId: 5, value: 2);
        var file = new Wdc5File(stream);
        file.TryGetRowById(5, out var row).ShouldBeTrue();

        file.ReadField<Wdc5TestEnum>(row, 0).ShouldBe(Wdc5TestEnum.Two);
    }

    [Fact]
    public void Wdc5File_ReadField_DoubleArray_ConvertsFromFloatArray()
    {
        using var stream = _fixture.CreateSingleSectionDenseNoneArrayFile(rowId: 7, floats: [1.5f, 2.5f, 3.5f]);
        var file = new Wdc5File(stream);
        file.TryGetRowById(7, out var row).ShouldBeTrue();

        file.ReadField<double[]>(row, 0).ShouldBe([1.5d, 2.5d, 3.5d]);
    }

    [Fact]
    public void Wdc5File_ReadField_ArrayTypes_CoversGetArrayBoxedBranches()
    {
        using var streamByte = _fixture.CreateSingleSectionDenseNoneArrayFile(rowId: 10, bytes: [1, 2, 3]);
        new Wdc5File(streamByte).ReadField<byte[]>(new RowHandle(0, 0, 10), 0).ShouldBe([1, 2, 3]);

        using var streamSByte = _fixture.CreateSingleSectionDenseNoneArrayFile(rowId: 11, sbytes: [1, -2, 3]);
        new Wdc5File(streamSByte).ReadField<sbyte[]>(new RowHandle(0, 0, 11), 0).ShouldBe([1, -2, 3]);

        using var streamUShort = _fixture.CreateSingleSectionDenseNoneArrayFile(rowId: 12, ushorts: [1, 2, 40000]);
        new Wdc5File(streamUShort).ReadField<ushort[]>(new RowHandle(0, 0, 12), 0).ShouldBe([1, 2, 40000]);

        using var streamLong = _fixture.CreateSingleSectionDenseNoneArrayFile(rowId: 13, longs: [1L, -2L, 3L]);
        new Wdc5File(streamLong).ReadField<long[]>(new RowHandle(0, 0, 13), 0).ShouldBe([1L, -2L, 3L]);

        using var streamULong = _fixture.CreateSingleSectionDenseNoneArrayFile(rowId: 14, ulongs: [1UL, 2UL, 3UL]);
        new Wdc5File(streamULong).ReadField<ulong[]>(new RowHandle(0, 0, 14), 0).ShouldBe([1UL, 2UL, 3UL]);
    }

    [Fact]
    public void Wdc5File_ReadField_SparseMultiField_SkipsEarlierFields()
    {
        using var stream = _fixture.CreateSingleSectionSparseMultiFieldInlineStringFile(rowId: 99, field0Value: 0x0A0B0C0D, field1StringBytes: [(byte)'h', (byte)'i', 0]);
        var file = new Wdc5File(stream);
        file.TryGetRowById(99, out var row).ShouldBeTrue();

        file.ReadField<string>(row, 1).ShouldBe("hi");
    }

    [Fact]
    public void Wdc5File_Ctor_FileTooSmall_Throws()
    {
        using var stream = new MemoryStream([1, 2, 3]);
        Should.Throw<InvalidDataException>(() => new Wdc5File(stream));
    }

    [Fact]
    public void Wdc5File_Ctor_AllSectionsEncrypted_NoKeyProvider_Throws()
    {
        using var stream = _fixture.CreateSingleSectionEncryptedImmediateScalarFile(rowId: 1, tactKeyLookup: 123, keyBytes: ReadOnlyMemory<byte>.Empty, value: 5);
        Should.Throw<NotSupportedException>(() => new Wdc5File(stream));
    }

    [Fact]
    public void Wdc5File_TryGetRowHandle_SupportsNintNuintAndThrowsOnUnsupportedType()
    {
        using var stream = CreateSingleSectionDenseScalarFile(flags: 0, idFieldIndex: 0, recordValue: 55);
        var file = new Wdc5File(stream);

        file.TryGetRowHandle((nint)55, out _).ShouldBeTrue();
        file.TryGetRowHandle((nuint)55, out _).ShouldBeTrue();
        file.TryGetRowHandle((byte)55, out _).ShouldBeTrue();
        file.TryGetRowHandle((sbyte)55, out _).ShouldBeTrue();

        Should.Throw<NotSupportedException>(() => file.TryGetRowHandle(1.23m, out _));
    }

    [Fact]
    public void Wdc5File_TryGetDenseStringTableIndex_InvalidRowHandle_Throws()
    {
        using var stream = CreateSingleSectionDenseStringFile(
            flags: 0,
            minIndex: 100,
            idFieldIndex: 0,
            recordValue: 5,
            stringTableBytes: [0, (byte)'h', (byte)'i', 0],
            indexData: [0],
            copyTable: []);

        var file = new Wdc5File(stream);

        Should.Throw<ArgumentException>(() => file.TryGetDenseStringTableIndex(new RowHandle(999, 0, 100), fieldIndex: 0, out _));
        Should.Throw<ArgumentException>(() => file.TryGetDenseStringTableIndex(new RowHandle(0, 999, 100), fieldIndex: 0, out _));
        Should.Throw<ArgumentOutOfRangeException>(() => file.TryGetDenseStringTableIndex(new RowHandle(0, 0, 100), fieldIndex: file.Header.FieldsCount, out _));
    }

    private static MemoryStream CreateMinimalWdc5Stream(uint layoutHash, int fieldsCount, int recordsCount, int sectionsCount, uint magic = 0x35434457)
    {
        var ms = new MemoryStream();
        using (var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write(magic);
            writer.Write(1u); // schemaVersion
            writer.Write(new byte[128]); // schemaString
            writer.Write(recordsCount);
            writer.Write(fieldsCount);
            writer.Write(0); // recordSize
            writer.Write(0); // stringTableSize
            writer.Write(0u); // tableHash
            writer.Write(layoutHash);
            writer.Write(0); // minIndex
            writer.Write(0); // maxIndex
            writer.Write(0); // locale
            writer.Write((ushort)0); // flags
            writer.Write((ushort)0); // idFieldIndex
            writer.Write(0); // totalFieldsCount
            writer.Write(0); // packedDataOffset
            writer.Write(0); // lookupColumnCount
            writer.Write(0); // columnMetaDataSize
            writer.Write(0); // commonDataSize
            writer.Write(0); // palletDataSize
            writer.Write(sectionsCount);

            // Section headers (none in our minimal streams).

            // Field meta (2x Int16 per field)
            for (var i = 0; i < fieldsCount; i++)
            {
                writer.Write((short)0);
                writer.Write((short)0);
            }

            // Column meta (24 bytes per field)
            if (fieldsCount != 0)
                writer.Write(new byte[fieldsCount * 24]);

            // Ensure the stream is at least the code's minimum header threshold.
            while (ms.Length < 204)
                writer.Write((byte)0);
        }

        ms.Position = 0;
        return ms;
    }

    private static MemoryStream CreateSingleSectionDenseStringFile(
        Db2Flags flags,
        int minIndex,
        ushort idFieldIndex,
        int recordValue,
        byte[] stringTableBytes,
        int[] indexData,
        (int DestinationId, int SourceId)[] copyTable)
    {
        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        const int fieldsCount = 1;
        const int recordsCount = 1;
        const int recordSize = 4;
        const int sectionsCount = 1;

        var sectionFileOffset = 512;

        WriteWdc5Header(
            writer,
            recordsCount: recordsCount,
            fieldsCount: fieldsCount,
            recordSize: recordSize,
            stringTableSize: stringTableBytes.Length,
            minIndex: minIndex,
            maxIndex: minIndex,
            flags: flags,
            idFieldIndex: idFieldIndex,
            sectionsCount: sectionsCount);

        // Section header
        WriteWdc5SectionHeader(writer, new Wdc5SectionHeader(
            TactKeyLookup: 0,
            FileOffset: sectionFileOffset,
            NumRecords: recordsCount,
            StringTableSize: stringTableBytes.Length,
            OffsetRecordsEndOffset: 0,
            IndexDataSize: indexData.Length * 4,
            ParentLookupDataSize: 0,
            OffsetMapIDCount: 0,
            CopyTableCount: copyTable.Length));

        // Field meta: Bits=0 => treated as string for boxed reads
        writer.Write((short)0);
        writer.Write((short)0);

        // Column meta: fixed 32-bit int at offset 0
        var columnMeta = new ColumnMetaData
        {
            RecordOffset = 0,
            Size = 32,
            AdditionalDataSize = 0,
            CompressionType = CompressionType.None,
        };

        WriteStruct(writer, columnMeta);

        PadTo(writer, sectionFileOffset);

        writer.Write(recordValue);
        writer.Write(stringTableBytes);

        foreach (var id in indexData)
            writer.Write(id);

        foreach (var (destinationId, sourceId) in copyTable)
        {
            writer.Write(destinationId);
            writer.Write(sourceId);
        }

        ms.Position = 0;
        return ms;
    }

    private static MemoryStream CreateTwoSectionDenseStringFile(
        int id0,
        byte[] stringTable0,
        int id1,
        byte[] stringTable1,
        int? recordValueOverride = null)
    {
        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        const int fieldsCount = 1;
        const int recordsCount = 2;
        const int recordSize = 4;
        const int sectionsCount = 2;

        var section0FileOffset = 512;
        var section1FileOffset = 768;

        var totalStringTableSize = stringTable0.Length + stringTable1.Length;
        var recordsBlobSizeBytes = recordsCount * recordSize;
        var recordValue = recordValueOverride ?? (recordsBlobSizeBytes + 1);

        WriteWdc5Header(
            writer,
            recordsCount: recordsCount,
            fieldsCount: fieldsCount,
            recordSize: recordSize,
            stringTableSize: totalStringTableSize,
            minIndex: Math.Min(id0, id1),
            maxIndex: Math.Max(id0, id1),
            flags: 0,
            idFieldIndex: 0,
            sectionsCount: sectionsCount);

        WriteWdc5SectionHeader(writer, new Wdc5SectionHeader(
            TactKeyLookup: 0,
            FileOffset: section0FileOffset,
            NumRecords: 1,
            StringTableSize: stringTable0.Length,
            OffsetRecordsEndOffset: 0,
            IndexDataSize: 4,
            ParentLookupDataSize: 0,
            OffsetMapIDCount: 0,
            CopyTableCount: 0));

        WriteWdc5SectionHeader(writer, new Wdc5SectionHeader(
            TactKeyLookup: 0,
            FileOffset: section1FileOffset,
            NumRecords: 1,
            StringTableSize: stringTable1.Length,
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
            CompressionType = CompressionType.None,
        };
        WriteStruct(writer, columnMeta);

        PadTo(writer, section0FileOffset);
        writer.Write(recordValue);
        writer.Write(stringTable0);
        writer.Write(id0);

        PadTo(writer, section1FileOffset);
        writer.Write(recordValue);
        writer.Write(stringTable1);
        writer.Write(id1);

        ms.Position = 0;
        return ms;
    }

    private static MemoryStream CreateSingleSectionDenseScalarFileWithParentLookup(Db2Flags flags, int rowId, int parentRelationId, int recordValue)
    {
        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        const int fieldsCount = 1;
        const int recordsCount = 1;
        const int recordSize = 4;
        const int sectionsCount = 1;

        var sectionFileOffset = 512;
        const int parentLookupDataSize = 24;

        WriteWdc5Header(
            writer,
            recordsCount: recordsCount,
            fieldsCount: fieldsCount,
            recordSize: recordSize,
            stringTableSize: 0,
            minIndex: rowId,
            maxIndex: rowId,
            flags: flags,
            idFieldIndex: 0,
            sectionsCount: sectionsCount);

        WriteWdc5SectionHeader(writer, new Wdc5SectionHeader(
            TactKeyLookup: 0,
            FileOffset: sectionFileOffset,
            NumRecords: recordsCount,
            StringTableSize: 0,
            OffsetRecordsEndOffset: 0,
            IndexDataSize: 4,
            ParentLookupDataSize: parentLookupDataSize,
            OffsetMapIDCount: 0,
            CopyTableCount: 0));

        writer.Write((short)32);
        writer.Write((short)0);

        var columnMeta = new ColumnMetaData
        {
            RecordOffset = 0,
            Size = 32,
            AdditionalDataSize = 0,
            CompressionType = CompressionType.None,
            Immediate = new ColumnCompressionDataImmediate(BitOffset: 0, BitWidth: 32, Flags: 0),
        };

        WriteStruct(writer, columnMeta);

        PadTo(writer, sectionFileOffset);
        writer.Write(recordValue);
        writer.Write(rowId);

        // Parent lookup data
        writer.Write(1); // numRecords
        writer.Write(rowId); // minId
        writer.Write(rowId); // maxId

        var parentLookupKey = flags.HasFlag(Db2Flags.SecondaryKey) ? rowId : 0;
        writer.Write(parentLookupKey);
        writer.Write(parentRelationId);

        ms.Position = 0;
        return ms;
    }

    private static MemoryStream CreateSingleSectionDenseTwoFieldCommonFile(int id0, int id1, uint defaultValue, int commonEntryId, uint commonEntryValue)
    {
        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        const int fieldsCount = 2;
        const int recordsCount = 2;
        const int recordSize = 4;
        const int sectionsCount = 1;

        var sectionFileOffset = 512;

        WriteWdc5Header(
            writer,
            recordsCount: recordsCount,
            fieldsCount: fieldsCount,
            recordSize: recordSize,
            stringTableSize: 0,
            minIndex: Math.Min(id0, id1),
            maxIndex: Math.Max(id0, id1),
            flags: 0,
            idFieldIndex: 0,
            sectionsCount: sectionsCount);

        WriteWdc5SectionHeader(writer, new Wdc5SectionHeader(
            TactKeyLookup: 0,
            FileOffset: sectionFileOffset,
            NumRecords: recordsCount,
            StringTableSize: 0,
            OffsetRecordsEndOffset: 0,
            IndexDataSize: 0,
            ParentLookupDataSize: 0,
            OffsetMapIDCount: 0,
            CopyTableCount: 0));

        // field meta
        writer.Write((short)0);
        writer.Write((short)0);
        writer.Write((short)0);
        writer.Write((short)0);

        // column meta: field 0 is ID, field 1 is Common with no bits stored.
        var idColumn = new ColumnMetaData
        {
            RecordOffset = 0,
            Size = 32,
            AdditionalDataSize = 0,
            CompressionType = CompressionType.None,
        };

        var commonColumn = new ColumnMetaData
        {
            RecordOffset = 32,
            Size = 32,
            AdditionalDataSize = 8,
            CompressionType = CompressionType.Common,
            Common = new ColumnCompressionDataCommon(DefaultValue: defaultValue, B: 0, C: 0),
        };

        WriteStruct(writer, idColumn);
        WriteStruct(writer, commonColumn);

        // pallet data for all columns (none)

        // common data for column 1
        writer.Write(commonEntryId);
        writer.Write(commonEntryValue);

        PadTo(writer, sectionFileOffset);
        writer.Write(id0);
        writer.Write(id1);

        ms.Position = 0;
        return ms;
    }

    private static MemoryStream CreateSingleSectionDenseTwoFieldPalletFile(int id0, int palletIndex0, int id1, int palletIndex1, uint[] palletData)
    {
        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        const int fieldsCount = 2;
        const int recordsCount = 2;
        const int recordSize = 8;
        const int sectionsCount = 1;

        var sectionFileOffset = 512;

        WriteWdc5Header(
            writer,
            recordsCount: recordsCount,
            fieldsCount: fieldsCount,
            recordSize: recordSize,
            stringTableSize: 0,
            minIndex: Math.Min(id0, id1),
            maxIndex: Math.Max(id0, id1),
            flags: 0,
            idFieldIndex: 0,
            sectionsCount: sectionsCount);

        WriteWdc5SectionHeader(writer, new Wdc5SectionHeader(
            TactKeyLookup: 0,
            FileOffset: sectionFileOffset,
            NumRecords: recordsCount,
            StringTableSize: 0,
            OffsetRecordsEndOffset: 0,
            IndexDataSize: 0,
            ParentLookupDataSize: 0,
            OffsetMapIDCount: 0,
            CopyTableCount: 0));

        writer.Write((short)0);
        writer.Write((short)0);
        writer.Write((short)0);
        writer.Write((short)0);

        var idColumn = new ColumnMetaData
        {
            RecordOffset = 0,
            Size = 32,
            AdditionalDataSize = 0,
            CompressionType = CompressionType.None,
        };

        var palletColumn = new ColumnMetaData
        {
            RecordOffset = 32,
            Size = 32,
            AdditionalDataSize = checked((uint)palletData.Length * 4u),
            CompressionType = CompressionType.Pallet,
            Pallet = new ColumnCompressionDataPallet(BitOffset: 0, BitWidth: 2, Cardinality: 1),
        };

        WriteStruct(writer, idColumn);
        WriteStruct(writer, palletColumn);

        foreach (var v in palletData)
            writer.Write(v);

        PadTo(writer, sectionFileOffset);

        writer.Write(id0);
        writer.Write(palletIndex0);

        writer.Write(id1);
        writer.Write(palletIndex1);

        ms.Position = 0;
        return ms;
    }

    private static MemoryStream CreateSingleSectionDenseTwoFieldPalletArrayFile(int rowId, int palletArrayIndex, int cardinality, uint[] palletData)
    {
        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        const int fieldsCount = 2;
        const int recordsCount = 1;
        const int recordSize = 8;
        const int sectionsCount = 1;

        var sectionFileOffset = 512;

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
            IndexDataSize: 0,
            ParentLookupDataSize: 0,
            OffsetMapIDCount: 0,
            CopyTableCount: 0));

        writer.Write((short)0);
        writer.Write((short)0);
        writer.Write((short)0);
        writer.Write((short)0);

        var idColumn = new ColumnMetaData
        {
            RecordOffset = 0,
            Size = 32,
            AdditionalDataSize = 0,
            CompressionType = CompressionType.None,
        };

        var arrayColumn = new ColumnMetaData
        {
            RecordOffset = 32,
            Size = checked((ushort)(cardinality * 32)),
            AdditionalDataSize = checked((uint)palletData.Length * 4u),
            CompressionType = CompressionType.PalletArray,
            Pallet = new ColumnCompressionDataPallet(BitOffset: 0, BitWidth: 2, Cardinality: cardinality),
        };

        WriteStruct(writer, idColumn);
        WriteStruct(writer, arrayColumn);

        foreach (var v in palletData)
            writer.Write(v);

        PadTo(writer, sectionFileOffset);
        writer.Write(rowId);
        writer.Write(palletArrayIndex);

        ms.Position = 0;
        return ms;
    }

    private static MemoryStream CreateSingleSectionSparseOffsetMapInlineStringFile(int row0Id, byte[] row0Bytes, int row1Id, byte[] row1Bytes)
    {
        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        const int fieldsCount = 1;
        const int recordsCount = 2;
        const int recordSize = 4;
        const int sectionsCount = 1;

        var sectionFileOffset = 512;
        var row0Offset = (uint)sectionFileOffset;
        var row1Offset = (uint)(sectionFileOffset + row0Bytes.Length);
        var recordDataSizeBytes = row0Bytes.Length + row1Bytes.Length;
        var offsetRecordsEndOffset = sectionFileOffset + recordDataSizeBytes;

        WriteWdc5Header(
            writer,
            recordsCount: recordsCount,
            fieldsCount: fieldsCount,
            recordSize: recordSize,
            stringTableSize: 0,
            minIndex: Math.Min(row0Id, row1Id),
            maxIndex: Math.Max(row0Id, row1Id),
            flags: Db2Flags.Sparse,
            idFieldIndex: 0,
            sectionsCount: sectionsCount);

        WriteWdc5SectionHeader(writer, new Wdc5SectionHeader(
            TactKeyLookup: 0,
            FileOffset: sectionFileOffset,
            NumRecords: recordsCount,
            StringTableSize: 0,
            OffsetRecordsEndOffset: offsetRecordsEndOffset,
            IndexDataSize: 0,
            ParentLookupDataSize: 0,
            OffsetMapIDCount: recordsCount,
            CopyTableCount: 0));

        writer.Write((short)0);
        writer.Write((short)0);

        var columnMeta = new ColumnMetaData
        {
            RecordOffset = 0,
            Size = 32,
            AdditionalDataSize = 0,
            CompressionType = CompressionType.None,
        };
        WriteStruct(writer, columnMeta);

        PadTo(writer, sectionFileOffset);
        writer.Write(row0Bytes);
        writer.Write(row1Bytes);

        WriteStruct(writer, new SparseEntry(Offset: row0Offset, Size: checked((ushort)row0Bytes.Length)));
        WriteStruct(writer, new SparseEntry(Offset: row1Offset, Size: checked((ushort)row1Bytes.Length)));

        writer.Write(row0Id);
        writer.Write(row1Id);

        ms.Position = 0;
        return ms;
    }

    private static MemoryStream CreateSingleSectionSparseInlineStringFile(int sparseRowId, byte[] rowBytes)
    {
        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        const int fieldsCount = 1;
        const int recordsCount = 1;
        const int recordSize = 4;
        const int sectionsCount = 1;

        var sectionFileOffset = 512;
        var offsetRecordsEndOffset = sectionFileOffset + rowBytes.Length;

        WriteWdc5Header(
            writer,
            recordsCount: recordsCount,
            fieldsCount: fieldsCount,
            recordSize: recordSize,
            stringTableSize: 0,
            minIndex: sparseRowId,
            maxIndex: sparseRowId,
            flags: Db2Flags.Sparse,
            idFieldIndex: 0,
            sectionsCount: sectionsCount);

        WriteWdc5SectionHeader(writer, new Wdc5SectionHeader(
            TactKeyLookup: 0,
            FileOffset: sectionFileOffset,
            NumRecords: recordsCount,
            StringTableSize: 0,
            OffsetRecordsEndOffset: offsetRecordsEndOffset,
            IndexDataSize: 0,
            ParentLookupDataSize: 0,
            OffsetMapIDCount: 1,
            CopyTableCount: 0));

        writer.Write((short)0);
        writer.Write((short)0);

        var columnMeta = new ColumnMetaData
        {
            RecordOffset = 0,
            Size = 32,
            AdditionalDataSize = 0,
            CompressionType = CompressionType.None,
        };
        WriteStruct(writer, columnMeta);

        PadTo(writer, sectionFileOffset);

        writer.Write(rowBytes);

        // Sparse entry + sparse index data (required when OffsetMapIDCount > 0)
        WriteStruct(writer, new SparseEntry(Offset: 0, Size: checked((ushort)rowBytes.Length)));
        writer.Write(sparseRowId);

        ms.Position = 0;
        return ms;
    }

    private static MemoryStream CreateSingleSectionDenseScalarFile(Db2Flags flags, ushort idFieldIndex, int recordValue)
    {
        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        const int fieldsCount = 1;
        const int recordsCount = 1;
        const int recordSize = 4;
        const int sectionsCount = 1;

        var sectionFileOffset = 512;

        WriteWdc5Header(
            writer,
            recordsCount: recordsCount,
            fieldsCount: fieldsCount,
            recordSize: recordSize,
            stringTableSize: 0,
            minIndex: recordValue,
            maxIndex: recordValue,
            flags: flags,
            idFieldIndex: idFieldIndex,
            sectionsCount: sectionsCount);

        WriteWdc5SectionHeader(writer, new Wdc5SectionHeader(
            TactKeyLookup: 0,
            FileOffset: sectionFileOffset,
            NumRecords: recordsCount,
            StringTableSize: 0,
            OffsetRecordsEndOffset: 0,
            IndexDataSize: 0,
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
            CompressionType = CompressionType.None,
        };

        WriteStruct(writer, columnMeta);

        PadTo(writer, sectionFileOffset);
        writer.Write(recordValue);

        ms.Position = 0;
        return ms;
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
        writer.Write(0x35434457u); // "WDC5"
        writer.Write(1u); // schemaVersion
        writer.Write(new byte[128]); // schemaString
        writer.Write(recordsCount);
        writer.Write(fieldsCount);
        writer.Write(recordSize);
        writer.Write(stringTableSize);
        writer.Write(0u); // tableHash
        writer.Write(0x12345678u); // layoutHash
        writer.Write(minIndex);
        writer.Write(maxIndex);
        writer.Write(0); // locale
        writer.Write((ushort)flags);
        writer.Write(idFieldIndex);
        writer.Write(0); // totalFieldsCount
        writer.Write(0); // packedDataOffset
        writer.Write(0); // lookupColumnCount
        writer.Write(0); // columnMetaDataSize
        writer.Write(0); // commonDataSize
        writer.Write(0); // palletDataSize
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
            writer.Write((byte)0);
    }

    private sealed class NonSeekableStream(Stream inner) : Stream
    {
        public override bool CanRead => inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => inner.CanWrite;
        public override long Length => inner.Length;
        public override long Position { get => inner.Position; set => throw new NotSupportedException(); }
        public override void Flush() => inner.Flush();
        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => inner.SetLength(value);
        public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);
        protected override void Dispose(bool disposing)
        {
            if (disposing)
                inner.Dispose();
            base.Dispose(disposing);
        }
    }
}

public enum Wdc5TestEnum : int
{
    Zero = 0,
    Two = 2,
}

public class Wdc5TestFixture
{
    public MemoryStream CreateSingleSectionDenseImmediateScalarFile(int rowId, int value)
    {
        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        const int fieldsCount = 1;
        const int recordsCount = 1;
        const int recordSize = 8;
        const int sectionsCount = 1;

        var sectionFileOffset = 512;

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

        writer.Write((short)1);
        writer.Write((short)0);

        var columnMeta = new ColumnMetaData
        {
            RecordOffset = 0,
            Size = 32,
            AdditionalDataSize = 0,
            CompressionType = CompressionType.Immediate,
            Immediate = new ColumnCompressionDataImmediate(BitOffset: 0, BitWidth: 32, Flags: 0),
        };
        WriteStruct(writer, columnMeta);

        PadTo(writer, sectionFileOffset);

        writer.Write(value);
        writer.Write(0);

        // index data: write 0 so Wdc5File expands it from minIndex.
        writer.Write(0);

        ms.Position = 0;
        return ms;
    }

    public MemoryStream CreateSingleSectionEncryptedImmediateScalarFile(int rowId, ulong tactKeyLookup, ReadOnlyMemory<byte> keyBytes, int value)
    {
        var record = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(record.AsSpan(0, 4), value);

        if (!keyBytes.IsEmpty)
        {
            Span<byte> nonce = stackalloc byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(nonce, unchecked((ulong)rowId));
            using var salsa = new Salsa20(keyBytes.Span, nonce);
            salsa.Transform(record, record);

            var anyNonZero = false;
            for (var i = 0; i < record.Length; i++)
            {
                if (record[i] != 0)
                {
                    anyNonZero = true;
                    break;
                }
            }

            if (!anyNonZero)
                record[0] = 1;
        }
        else
        {
            // Keep it non-zero so the constructor doesn't treat it as a placeholder.
            record[0] = 1;
        }

        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        const int fieldsCount = 1;
        const int recordsCount = 1;
        const int recordSize = 8;
        const int sectionsCount = 1;

        var sectionFileOffset = 512;

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
            TactKeyLookup: tactKeyLookup,
            FileOffset: sectionFileOffset,
            NumRecords: recordsCount,
            StringTableSize: 0,
            OffsetRecordsEndOffset: 0,
            IndexDataSize: 4,
            ParentLookupDataSize: 0,
            OffsetMapIDCount: 0,
            CopyTableCount: 0));

        writer.Write((short)1);
        writer.Write((short)0);

        var columnMeta = new ColumnMetaData
        {
            RecordOffset = 0,
            Size = 32,
            AdditionalDataSize = 0,
            CompressionType = CompressionType.Immediate,
            Immediate = new ColumnCompressionDataImmediate(BitOffset: 0, BitWidth: 32, Flags: 0),
        };
        WriteStruct(writer, columnMeta);

        PadTo(writer, sectionFileOffset);

        writer.Write(record);
        writer.Write(0);

        ms.Position = 0;
        return ms;
    }

    public MemoryStream CreateSingleSectionDenseNoneArrayFile(
        int rowId,
        byte[]? bytes = null,
        sbyte[]? sbytes = null,
        ushort[]? ushorts = null,
        long[]? longs = null,
        ulong[]? ulongs = null,
        float[]? floats = null)
    {
        object?[] provided = [bytes, sbytes, ushorts, longs, ulongs, floats];
        var providedCount = provided.Count(x => x is not null);
        if (providedCount != 1)
            throw new ArgumentException("Exactly one array payload must be provided.");

        ushort elementBits;
        byte[] recordBytes;
        ushort sizeBits;

        if (bytes is not null)
        {
            elementBits = 8;
            recordBytes = bytes;
            sizeBits = checked((ushort)(bytes.Length * 8));
        }
        else if (sbytes is not null)
        {
            elementBits = 8;
            recordBytes = sbytes.Select(x => unchecked((byte)x)).ToArray();
            sizeBits = checked((ushort)(sbytes.Length * 8));
        }
        else if (ushorts is not null)
        {
            elementBits = 16;
            recordBytes = new byte[ushorts.Length * 2];
            for (var i = 0; i < ushorts.Length; i++)
                BinaryPrimitives.WriteUInt16LittleEndian(recordBytes.AsSpan(i * 2, 2), ushorts[i]);
            sizeBits = checked((ushort)(ushorts.Length * 16));
        }
        else if (longs is not null)
        {
            elementBits = 64;
            recordBytes = new byte[longs.Length * 8];
            for (var i = 0; i < longs.Length; i++)
                BinaryPrimitives.WriteInt64LittleEndian(recordBytes.AsSpan(i * 8, 8), longs[i]);
            sizeBits = checked((ushort)(longs.Length * 64));
        }
        else if (ulongs is not null)
        {
            elementBits = 64;
            recordBytes = new byte[ulongs.Length * 8];
            for (var i = 0; i < ulongs.Length; i++)
                BinaryPrimitives.WriteUInt64LittleEndian(recordBytes.AsSpan(i * 8, 8), ulongs[i]);
            sizeBits = checked((ushort)(ulongs.Length * 64));
        }
        else if (floats is not null)
        {
            elementBits = 32;
            recordBytes = new byte[floats.Length * 4];
            for (var i = 0; i < floats.Length; i++)
                BinaryPrimitives.WriteSingleLittleEndian(recordBytes.AsSpan(i * 4, 4), floats[i]);
            sizeBits = checked((ushort)(floats.Length * 32));
        }
        else
        {
            throw new InvalidOperationException("Unreachable: no payload.");
        }

        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        const int fieldsCount = 1;
        const int recordsCount = 1;
        const int sectionsCount = 1;
        var sectionFileOffset = 512;
        var recordSize = recordBytes.Length;

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

        var bits = elementBits == 64 ? (short)32 : (short)(32 - elementBits);
        writer.Write(bits);
        writer.Write((short)0);

        var columnMeta = new ColumnMetaData
        {
            RecordOffset = 0,
            Size = sizeBits,
            AdditionalDataSize = 0,
            CompressionType = CompressionType.None,
            Immediate = new ColumnCompressionDataImmediate(BitOffset: 0, BitWidth: elementBits, Flags: 0),
        };
        WriteStruct(writer, columnMeta);

        PadTo(writer, sectionFileOffset);
        writer.Write(recordBytes);
        writer.Write(0);

        ms.Position = 0;
        return ms;
    }

    public MemoryStream CreateSingleSectionSparseMultiFieldInlineStringFile(int rowId, int field0Value, byte[] field1StringBytes)
    {
        var rowBytes = new byte[4 + field1StringBytes.Length];
        BinaryPrimitives.WriteInt32LittleEndian(rowBytes.AsSpan(0, 4), field0Value);
        field1StringBytes.CopyTo(rowBytes.AsSpan(4));

        var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        const int fieldsCount = 2;
        const int recordsCount = 1;
        const int sectionsCount = 1;

        var sectionFileOffset = 512;
        var recordDataSizeBytes = rowBytes.Length;
        var offsetRecordsEndOffset = sectionFileOffset + recordDataSizeBytes;

        WriteWdc5Header(
            writer,
            recordsCount: recordsCount,
            fieldsCount: fieldsCount,
            recordSize: 0,
            stringTableSize: 0,
            minIndex: rowId,
            maxIndex: rowId,
            flags: Db2Flags.Sparse,
            idFieldIndex: 0,
            sectionsCount: sectionsCount);

        WriteWdc5SectionHeader(writer, new Wdc5SectionHeader(
            TactKeyLookup: 0,
            FileOffset: sectionFileOffset,
            NumRecords: recordsCount,
            StringTableSize: 0,
            OffsetRecordsEndOffset: offsetRecordsEndOffset,
            IndexDataSize: 0,
            ParentLookupDataSize: 0,
            OffsetMapIDCount: 1,
            CopyTableCount: 0));

        // Field 0: immediate 32-bit scalar.
        writer.Write((short)1);
        writer.Write((short)0);

        // Field 1: none (inline string data)
        writer.Write((short)0);
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
            CompressionType = CompressionType.None,
            Immediate = new ColumnCompressionDataImmediate(BitOffset: 0, BitWidth: 32, Flags: 0),
        };

        WriteStruct(writer, field0);
        WriteStruct(writer, field1);

        PadTo(writer, sectionFileOffset);
        writer.Write(rowBytes);

        WriteStruct(writer, new SparseEntry(Offset: 0, Size: checked((ushort)recordDataSizeBytes)));
        writer.Write(rowId);

        ms.Position = 0;
        return ms;
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
        writer.Write(0x35434457u); // "WDC5"
        writer.Write(1u); // schemaVersion
        writer.Write(new byte[128]); // schemaString
        writer.Write(recordsCount);
        writer.Write(fieldsCount);
        writer.Write(recordSize);
        writer.Write(stringTableSize);
        writer.Write(0u); // tableHash
        writer.Write(0x12345678u); // layoutHash
        writer.Write(minIndex);
        writer.Write(maxIndex);
        writer.Write(0); // locale
        writer.Write((ushort)flags);
        writer.Write(idFieldIndex);
        writer.Write(0); // totalFieldsCount
        writer.Write(0); // packedDataOffset
        writer.Write(0); // lookupColumnCount
        writer.Write(0); // columnMetaDataSize
        writer.Write(0); // commonDataSize
        writer.Write(0); // palletDataSize
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
            writer.Write((byte)0);
    }
}
