namespace MimironSQL.Formats;

/// <summary>
/// Resolves dense string table indexes for row fields.
/// </summary>
/// <typeparam name="TRow">The row struct type.</typeparam>
public interface IDb2DenseStringTableIndexProvider<TRow> where TRow : struct
{
    /// <summary>
    /// Attempts to resolve the string table index for a field within a row.
    /// </summary>
    /// <param name="row">The row value.</param>
    /// <param name="fieldIndex">The zero-based field index.</param>
    /// <param name="stringTableIndex">When successful, receives the string table index.</param>
    /// <returns><see langword="true"/> if a string index is available; otherwise <see langword="false"/>.</returns>
    bool TryGetDenseStringTableIndex(TRow row, int fieldIndex, out int stringTableIndex);
}
