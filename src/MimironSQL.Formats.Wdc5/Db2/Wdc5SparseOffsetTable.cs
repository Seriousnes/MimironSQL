namespace MimironSQL.Formats.Wdc5.Db2;

internal sealed class Wdc5SparseOffsetTable
{
    private readonly int[] _fieldBitPositions;

    internal Wdc5SparseOffsetTable(int fieldCount, int rowCount, int[] fieldBitPositions)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(fieldCount);
        ArgumentOutOfRangeException.ThrowIfNegative(rowCount);
        ArgumentNullException.ThrowIfNull(fieldBitPositions);

        if (fieldBitPositions.Length != fieldCount * rowCount)
        {
            throw new ArgumentException("Sparse field offset table length does not match the expected row/field count.", nameof(fieldBitPositions));
        }

        FieldCount = fieldCount;
        RowCount = rowCount;
        _fieldBitPositions = fieldBitPositions;
    }

    internal int FieldCount { get; }

    internal int RowCount { get; }

    internal int GetFieldBitPosition(int rowIndex, int fieldIndex)
    {
        if ((uint)rowIndex >= (uint)RowCount)
        {
            throw new ArgumentOutOfRangeException(nameof(rowIndex));
        }

        if ((uint)fieldIndex >= (uint)FieldCount)
        {
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));
        }

        return _fieldBitPositions[(rowIndex * FieldCount) + fieldIndex];
    }
}