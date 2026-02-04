namespace MimironSQL.Formats;

public interface IDb2DenseStringTableIndexProvider<TRow> where TRow : struct
{
    bool TryGetDenseStringTableIndex(TRow row, int fieldIndex, out int stringTableIndex);
}
