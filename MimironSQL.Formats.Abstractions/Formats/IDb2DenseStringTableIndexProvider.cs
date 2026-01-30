namespace MimironSQL.Formats;

public interface IDb2DenseStringTableIndexProvider<TRow> where TRow : struct, IDb2Row
{
    bool TryGetDenseStringTableIndex(TRow row, int fieldIndex, out int stringTableIndex);
}
