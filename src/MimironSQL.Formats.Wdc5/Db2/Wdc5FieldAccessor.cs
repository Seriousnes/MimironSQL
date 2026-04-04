using MimironSQL.Db2;

namespace MimironSQL.Formats.Wdc5.Db2;

internal ref struct Wdc5FieldAccessor
{
    private readonly Wdc5File _file;
    private readonly Wdc5Section _section;
    private readonly ReadOnlySpan<byte> _records;
    private readonly int _fieldIndex;
    private readonly int _recordSizeBits;
    private readonly int _fieldBitOffset;
    private readonly FieldMetaData _fieldMeta;
    private readonly ColumnMetaData _columnMeta;
    private readonly uint[] _palletData;
    private readonly Dictionary<int, uint> _commonData;
    private readonly bool _needsSourceId;

    internal Wdc5FieldAccessor(Wdc5File file, Wdc5Section section, int fieldIndex)
    {
        ArgumentNullException.ThrowIfNull(file);
        ArgumentNullException.ThrowIfNull(section);

        if (section.IsDecryptable)
        {
            throw new NotSupportedException("Dense field accessors do not support encrypted sections.");
        }

        if (file.Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            throw new NotSupportedException("Dense field accessors cannot be created for sparse WDC5 files.");
        }

        if ((uint)fieldIndex >= (uint)file.Header.FieldsCount)
        {
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));
        }

        if (section.RecordsData is null)
        {
            throw new InvalidOperationException("Dense field accessors require materialized section records.");
        }

        var columnMeta = file.ColumnMeta[fieldIndex];
        if (columnMeta.CompressionType is CompressionType.PalletArray && columnMeta.Pallet.Cardinality is not 1)
        {
            throw new NotSupportedException("Dense field accessors only support scalar columns.");
        }

        _file = file;
        _section = section;
        _records = section.RecordsData;
        _fieldIndex = fieldIndex;
        _recordSizeBits = file.Header.RecordSize * 8;
        _fieldBitOffset = columnMeta.RecordOffset;
        _fieldMeta = file.FieldMeta[fieldIndex];
        _columnMeta = columnMeta;
        _palletData = file.PalletData[fieldIndex];
        _commonData = file.CommonData[fieldIndex];
        _needsSourceId = columnMeta.CompressionType == CompressionType.Common;
    }

    internal T ReadScalar<T>(int rowIndex) where T : unmanaged
    {
        var reader = CreateReader(rowIndex, out var sourceId);
        return Wdc5FieldDecoder.ReadScalar<T>(sourceId, ref reader, _fieldMeta, _columnMeta, _palletData, _commonData);
    }

    internal ulong ReadScalarRaw(int rowIndex)
    {
        var reader = CreateReader(rowIndex, out var sourceId);
        return Wdc5FieldDecoder.ReadScalarRaw(sourceId, ref reader, _fieldMeta, _columnMeta, _palletData, _commonData);
    }

    private Wdc5RowReader CreateReader(int rowIndex, out int sourceId)
    {
        if ((uint)rowIndex >= (uint)_section.NumRecords)
        {
            throw new ArgumentOutOfRangeException(nameof(rowIndex));
        }

        sourceId = _needsSourceId ? _file.GetSourceIdForAccessor(_section, rowIndex) : 0;
        return new Wdc5RowReader(_records, positionBits: (rowIndex * _recordSizeBits) + _fieldBitOffset);
    }
}