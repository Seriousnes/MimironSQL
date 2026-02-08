using MimironSQL.Formats;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

internal static class Db2RowHandleAccess
{
    public static RowHandle AsHandle<TRow>(TRow row) where TRow : struct, IRowHandle
        => row.Handle;

    public static T ReadField<TRow, T>(IDb2File file, TRow row, int fieldIndex) where TRow : struct, IRowHandle
        => file.ReadField<T>(row.Handle, fieldIndex);
}
