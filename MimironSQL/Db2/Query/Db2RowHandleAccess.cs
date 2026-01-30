using System.Runtime.CompilerServices;

using MimironSQL.Formats;

namespace MimironSQL.Db2.Query;

internal static class Db2RowHandleAccess
{
    public static RowHandle AsHandle<TRow>(TRow row) where TRow : struct
        => Unsafe.As<TRow, RowHandle>(ref row);

    public static T ReadField<TRow, T>(IDb2File file, TRow row, int fieldIndex) where TRow : struct
        => file.ReadField<T>(AsHandle(row), fieldIndex);
}
