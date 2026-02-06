using Shouldly;

namespace MimironSQL.Formats.Wdc5.Tests;

public sealed class Wdc5FieldDecoderTests
{
    private readonly struct Big16
    {
        public readonly long A;
        public readonly long B;

        public Big16(long a, long b)
        {
            A = a;
            B = b;
        }
    }

    [Fact]
    public void ReadScalar_None_uses_immediate_bit_width_when_field_bits_make_bit_size_non_positive()
    {
        var fieldMeta = new FieldMetaData(Bits: 32, Offset: 0);
        var columnMeta = new ColumnMetaData
        {
            CompressionType = CompressionType.None,
            Immediate = new ColumnCompressionDataImmediate(BitOffset: 0, BitWidth: 8, Flags: 0),
        };

        var reader = new Wdc5RowReader(bytes: [42], positionBits: 0);

        Wdc5FieldDecoder.ReadScalar<uint>(
            id: 0,
            reader: ref reader,
            fieldMeta: fieldMeta,
            columnMeta: columnMeta,
            palletData: [],
            commonData: new Dictionary<int, uint>())
            .ShouldBe(42u);
    }

    [Fact]
    public void ReadScalar_SignedImmediate_reads_signed_bits()
    {
        var fieldMeta = new FieldMetaData(Bits: 0, Offset: 0);
        var columnMeta = new ColumnMetaData
        {
            CompressionType = CompressionType.SignedImmediate,
            Immediate = new ColumnCompressionDataImmediate(BitOffset: 0, BitWidth: 3, Flags: 0),
        };

        var reader = new Wdc5RowReader(bytes: [0b0000_0111], positionBits: 0);

        Wdc5FieldDecoder.ReadScalar<int>(
            id: 0,
            reader: ref reader,
            fieldMeta: fieldMeta,
            columnMeta: columnMeta,
            palletData: [],
            commonData: new Dictionary<int, uint>())
            .ShouldBe(-1);
    }

    [Fact]
    public void ReadScalar_Immediate_reads_unsigned_bits()
    {
        var fieldMeta = new FieldMetaData(Bits: 0, Offset: 0);
        var columnMeta = new ColumnMetaData
        {
            CompressionType = CompressionType.Immediate,
            Immediate = new ColumnCompressionDataImmediate(BitOffset: 0, BitWidth: 4, Flags: 0),
        };

        var reader = new Wdc5RowReader(bytes: [0b0000_1111], positionBits: 0);

        Wdc5FieldDecoder.ReadScalar<uint>(
            id: 0,
            reader: ref reader,
            fieldMeta: fieldMeta,
            columnMeta: columnMeta,
            palletData: [],
            commonData: new Dictionary<int, uint>())
            .ShouldBe(15u);
    }

    [Fact]
    public void ReadScalar_Common_prefers_per_id_value_and_falls_back_to_default()
    {
        var fieldMeta = new FieldMetaData(Bits: 0, Offset: 0);
        var columnMeta = new ColumnMetaData
        {
            CompressionType = CompressionType.Common,
            Common = new ColumnCompressionDataCommon(DefaultValue: 77u, B: 0, C: 0),
        };

        var commonData = new Dictionary<int, uint>
        {
            [123] = 456u,
        };

        var reader = new Wdc5RowReader(bytes: [], positionBits: 0);

        Wdc5FieldDecoder.ReadScalar<uint>(123, ref reader, fieldMeta, columnMeta, palletData: [], commonData)
            .ShouldBe(456u);

        Wdc5FieldDecoder.ReadScalar<uint>(999, ref reader, fieldMeta, columnMeta, palletData: [], commonData)
            .ShouldBe(77u);
    }

    [Fact]
    public void ReadScalar_Pallet_reads_from_pallet_data()
    {
        var fieldMeta = new FieldMetaData(Bits: 0, Offset: 0);
        var columnMeta = new ColumnMetaData
        {
            CompressionType = CompressionType.Pallet,
            Pallet = new ColumnCompressionDataPallet(BitOffset: 0, BitWidth: 2, Cardinality: 1),
        };

        var reader = new Wdc5RowReader(bytes: [0b0000_0010], positionBits: 0);

        Wdc5FieldDecoder.ReadScalar<uint>(
            id: 0,
            reader: ref reader,
            fieldMeta: fieldMeta,
            columnMeta: columnMeta,
            palletData: [10u, 20u, 30u, 40u],
            commonData: new Dictionary<int, uint>())
            .ShouldBe(30u);
    }

    [Fact]
    public void ReadScalar_PalletArray_with_cardinality_1_reads_from_pallet_data()
    {
        var fieldMeta = new FieldMetaData(Bits: 0, Offset: 0);
        var columnMeta = new ColumnMetaData
        {
            CompressionType = CompressionType.PalletArray,
            Pallet = new ColumnCompressionDataPallet(BitOffset: 0, BitWidth: 2, Cardinality: 1),
        };

        var reader = new Wdc5RowReader(bytes: [0b0000_0001], positionBits: 0);

        Wdc5FieldDecoder.ReadScalar<uint>(
            id: 0,
            reader: ref reader,
            fieldMeta: fieldMeta,
            columnMeta: columnMeta,
            palletData: [10u, 20u, 30u, 40u],
            commonData: new Dictionary<int, uint>())
            .ShouldBe(20u);
    }

    [Fact]
    public void ReadScalar_PalletArray_with_non_1_cardinality_returns_default()
    {
        var fieldMeta = new FieldMetaData(Bits: 0, Offset: 0);
        var columnMeta = new ColumnMetaData
        {
            CompressionType = CompressionType.PalletArray,
            Pallet = new ColumnCompressionDataPallet(BitOffset: 0, BitWidth: 2, Cardinality: 2),
        };

        var reader = new Wdc5RowReader(bytes: [0b0000_0001], positionBits: 0);

        Wdc5FieldDecoder.ReadScalar<uint>(
            id: 0,
            reader: ref reader,
            fieldMeta: fieldMeta,
            columnMeta: columnMeta,
            palletData: [10u, 20u, 30u, 40u],
            commonData: new Dictionary<int, uint>())
            .ShouldBe(default);
    }

    [Fact]
    public void ReadScalar_unsupported_compression_type_throws()
    {
        var fieldMeta = new FieldMetaData(Bits: 0, Offset: 0);
        var columnMeta = new ColumnMetaData { CompressionType = (CompressionType)123 };

        Should.Throw<NotSupportedException>(() =>
        {
            var reader = new Wdc5RowReader(bytes: [], positionBits: 0);
            Wdc5FieldDecoder.ReadScalar<uint>(
                id: 0,
                reader: ref reader,
                fieldMeta: fieldMeta,
                columnMeta: columnMeta,
                palletData: [],
                commonData: new Dictionary<int, uint>());
        });
    }

    [Fact]
    public void ReadScalar_unsupported_scalar_size_throws()
    {
        var fieldMeta = new FieldMetaData(Bits: 0, Offset: 0);
        var columnMeta = new ColumnMetaData
        {
            CompressionType = CompressionType.Immediate,
            Immediate = new ColumnCompressionDataImmediate(BitOffset: 0, BitWidth: 1, Flags: 0),
        };

        Should.Throw<NotSupportedException>(() =>
        {
            var reader = new Wdc5RowReader(bytes: [0b0000_0001], positionBits: 0);
            Wdc5FieldDecoder.ReadScalar<Big16>(
                id: 0,
                reader: ref reader,
                fieldMeta: fieldMeta,
                columnMeta: columnMeta,
                palletData: [],
                commonData: new Dictionary<int, uint>());
        });
    }
}
