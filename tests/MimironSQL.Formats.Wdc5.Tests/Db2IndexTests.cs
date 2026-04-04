using Shouldly;

using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5.Index;

namespace MimironSQL.Formats.Wdc5.Tests;

public sealed class Db2IndexTests
{
    [Fact]
    public void EncodeDecode_RoundTripsSupportedScalarTypes()
    {
        Db2IndexValueEncoder.Decode<int>(Db2IndexValueEncoder.Encode(-123)).ShouldBe(-123);
        Db2IndexValueEncoder.Decode<uint>(Db2IndexValueEncoder.Encode(123u)).ShouldBe(123u);
        Db2IndexValueEncoder.Decode<long>(Db2IndexValueEncoder.Encode(-1234567890123L)).ShouldBe(-1234567890123L);
        Db2IndexValueEncoder.Decode<ulong>(Db2IndexValueEncoder.Encode(1234567890123UL)).ShouldBe(1234567890123UL);
        Db2IndexValueEncoder.Decode<short>(Db2IndexValueEncoder.Encode((short)-321)).ShouldBe((short)-321);
        Db2IndexValueEncoder.Decode<ushort>(Db2IndexValueEncoder.Encode((ushort)321)).ShouldBe((ushort)321);
        Db2IndexValueEncoder.Decode<sbyte>(Db2IndexValueEncoder.Encode((sbyte)-12)).ShouldBe((sbyte)-12);
        Db2IndexValueEncoder.Decode<byte>(Db2IndexValueEncoder.Encode((byte)12)).ShouldBe((byte)12);
        Db2IndexValueEncoder.Decode<float>(Db2IndexValueEncoder.Encode(-12.5f)).ShouldBe(-12.5f);
        Db2IndexValueEncoder.Decode<double>(Db2IndexValueEncoder.Encode(12.5d)).ShouldBe(12.5d);
    }

    [Fact]
    public void Encode_PreservesSortOrder_ForSignedIntegersAndFloats()
    {
        var intValues = new[] { -20, -1, 0, 1, 20 };
        var encodedInts = intValues.Select(Db2IndexValueEncoder.Encode).ToArray();
        encodedInts.ShouldBe(encodedInts.OrderBy(static x => x).ToArray());

        var floatValues = new[] { -20.5f, -1.25f, 0f, 1.25f, 20.5f };
        var encodedFloats = floatValues.Select(Db2IndexValueEncoder.Encode).ToArray();
        encodedFloats.ShouldBe(encodedFloats.OrderBy(static x => x).ToArray());
    }

    [Fact]
    public void WriteAndRead_EqualityAndRangeLookups_ReturnExpectedRows()
    {
        var filePath = GetTemporaryFilePath();
        try
        {
            List<(ulong EncodedValue, ushort SectionIndex, int RowIndex)> entries =
            [
                (Db2IndexValueEncoder.Encode(10), 0, 0),
                (Db2IndexValueEncoder.Encode(20), 0, 1),
                (Db2IndexValueEncoder.Encode(20), 1, 0),
                (Db2IndexValueEncoder.Encode(30), 1, 1)
            ];

            Db2IndexWriter.Write(
                filePath,
                entries,
                valueByteWidth: 4,
                tableName: "TestTable",
                fieldIndex: 0,
                valueType: 0,
                wowVersion: "12.0.0.65655",
                layoutHash: 0x12345678u);

            using var reader = new Db2IndexReader(filePath);

            reader.RecordCount.ShouldBe(4);
            reader.FindEquals(Db2IndexValueEncoder.Encode(20)).ShouldBe(
            [
                new RowHandle(0, 1, 0),
                new RowHandle(1, 0, 0)
            ]);

            reader.FindRange(Db2IndexValueEncoder.Encode(15), Db2IndexValueEncoder.Encode(25)).ShouldBe(
            [
                new RowHandle(0, 1, 0),
                new RowHandle(1, 0, 0)
            ]);
        }
        finally
        {
            TryDelete(filePath);
        }
    }

    [Fact]
    public void FindEquals_ReturnsAllDuplicateKeysAcrossLeafPages()
    {
        var filePath = GetTemporaryFilePath();
        try
        {
            var duplicateCount = Db2IndexFileFormat.GetMaxLeafEntries(4) + 10;
            var target = Db2IndexValueEncoder.Encode(42);
            var entries = Enumerable.Range(0, duplicateCount)
                .Select(static index => (EncodedValue: Db2IndexValueEncoder.Encode(42), SectionIndex: (ushort)(index % 2), RowIndex: index))
                .ToList();

            Db2IndexWriter.Write(
                filePath,
                entries,
                valueByteWidth: 4,
                tableName: "TestTable",
                fieldIndex: 0,
                valueType: 0,
                wowVersion: "12.0.0.65655",
                layoutHash: 0x12345678u);

            using var reader = new Db2IndexReader(filePath);

            reader.TreeHeight.ShouldBeGreaterThan(1);
            reader.FindEquals(target).Count.ShouldBe(duplicateCount);
            reader.FindRange(target, target).Count.ShouldBe(duplicateCount);
        }
        finally
        {
            TryDelete(filePath);
        }
    }

    private static string GetTemporaryFilePath()
        => Path.Combine(Path.GetTempPath(), $"mimironsql-{Guid.NewGuid():N}.db2idx");

    private static void TryDelete(string filePath)
    {
        try
        {
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }
        }
        catch
        {
        }
    }
}
