using MimironSQL.Db2;

namespace MimironSQL.Formats.Wdc5.Db2;

internal ref struct Wdc5SparseFieldAccessor
{
    private readonly Wdc5File _file;
    private readonly Wdc5Section _section;
    private readonly Wdc5SparseOffsetTable _offsetTable;
    private readonly ReadOnlySpan<byte> _records;
    private readonly FieldMetaData _fieldMeta;
    private readonly ColumnMetaData _columnMeta;
    private readonly uint[] _palletData;
    private readonly Dictionary<int, uint> _commonData;
    private readonly int _fieldIndex;
    private readonly bool _needsSourceId;

    internal Wdc5SparseFieldAccessor(Wdc5File file, Wdc5Section section, int fieldIndex, Wdc5SparseOffsetTable offsetTable)
    {
        ArgumentNullException.ThrowIfNull(file);
        ArgumentNullException.ThrowIfNull(section);
        ArgumentNullException.ThrowIfNull(offsetTable);

        if (section.IsDecryptable)
        {
            throw new NotSupportedException("Sparse field accessors do not support encrypted sections.");
        }

        if (!file.Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            throw new NotSupportedException("Sparse field accessors require sparse WDC5 files.");
        }

        if ((uint)fieldIndex >= (uint)file.Header.FieldsCount)
        {
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));
        }

        if (section.RecordsData is null)
        {
            throw new InvalidOperationException("Sparse field accessors require materialized section records.");
        }

        var columnMeta = file.ColumnMeta[fieldIndex];
        if (columnMeta.CompressionType is CompressionType.PalletArray && columnMeta.Pallet.Cardinality is not 1)
        {
            throw new NotSupportedException("Sparse field accessors only support scalar columns.");
        }

        _file = file;
        _section = section;
        _offsetTable = offsetTable;
        _records = section.RecordsData;
        _fieldMeta = file.FieldMeta[fieldIndex];
        _columnMeta = columnMeta;
        _palletData = file.PalletData[fieldIndex];
        _commonData = file.CommonData[fieldIndex];
        _fieldIndex = fieldIndex;
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
        var positionBits = _offsetTable.GetFieldBitPosition(rowIndex, _fieldIndex);
        return new Wdc5RowReader(_records, positionBits: positionBits);
    }
}