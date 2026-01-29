using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;

namespace MimironSQL.Db2.Query;

internal static class Db2RowValue
{
    public static string ReadString(Wdc5Row row, Db2FieldAccessor accessor)
    {
        var field = accessor.Field;

        if (field.IsVirtual)
            throw new NotSupportedException($"Virtual field '{field.Name}' cannot be materialized as a string.");

        _ = row.TryGetString(field.ColumnStartIndex, out var str);
        return str;
    }

    public static float ReadSingle(Wdc5Row row, Db2FieldAccessor accessor)
    {
        var field = accessor.Field;

        if (field.IsVirtual)
            return GetVirtualNumeric(row, field);

        return field is { ValueType: Db2ValueType.Single }
            ? row.GetScalar<float>(field.ColumnStartIndex)
            : Convert.ToSingle(ReadNumericInt64OrUInt64(row, field));
    }

    public static double ReadDouble(Wdc5Row row, Db2FieldAccessor accessor)
    {
        var field = accessor.Field;

        if (field.IsVirtual)
            return GetVirtualNumeric(row, field);

        return Convert.ToDouble(ReadNumericInt64OrUInt64(row, field));
    }

    public static bool ReadBoolean(Wdc5Row row, Db2FieldAccessor accessor)
        => ReadInt64(row, accessor) != 0;

    public static byte ReadByte(Wdc5Row row, Db2FieldAccessor accessor)
    {
        var field = accessor.Field;

        if (field.IsVirtual)
            return Convert.ToByte(GetVirtualNumeric(row, field));

        return field.ValueType == Db2ValueType.UInt64
            ? Convert.ToByte(ReadNumericUInt64(row, field))
            : Convert.ToByte(ReadNumericInt64(row, field));
    }

    public static sbyte ReadSByte(Wdc5Row row, Db2FieldAccessor accessor)
    {
        var field = accessor.Field;

        if (field.IsVirtual)
            return Convert.ToSByte(GetVirtualNumeric(row, field));

        return field.ValueType == Db2ValueType.UInt64
            ? Convert.ToSByte(ReadNumericUInt64(row, field))
            : Convert.ToSByte(ReadNumericInt64(row, field));
    }

    public static short ReadInt16(Wdc5Row row, Db2FieldAccessor accessor)
    {
        var field = accessor.Field;

        if (field.IsVirtual)
            return Convert.ToInt16(GetVirtualNumeric(row, field));

        return field.ValueType == Db2ValueType.UInt64
            ? Convert.ToInt16(ReadNumericUInt64(row, field))
            : Convert.ToInt16(ReadNumericInt64(row, field));
    }

    public static ushort ReadUInt16(Wdc5Row row, Db2FieldAccessor accessor)
    {
        var field = accessor.Field;

        if (field.IsVirtual)
            return Convert.ToUInt16(GetVirtualNumeric(row, field));

        return field.ValueType == Db2ValueType.UInt64
            ? Convert.ToUInt16(ReadNumericUInt64(row, field))
            : Convert.ToUInt16(ReadNumericInt64(row, field));
    }

    public static int ReadInt32(Wdc5Row row, Db2FieldAccessor accessor)
    {
        var field = accessor.Field;

        if (field.IsVirtual)
            return Convert.ToInt32(GetVirtualNumeric(row, field));

        return field.ValueType == Db2ValueType.UInt64
            ? Convert.ToInt32(ReadNumericUInt64(row, field))
            : Convert.ToInt32(ReadNumericInt64(row, field));
    }

    public static uint ReadUInt32(Wdc5Row row, Db2FieldAccessor accessor)
    {
        var field = accessor.Field;

        if (field.IsVirtual)
            return GetVirtualNumericUInt32(row, field);

        return field.ValueType == Db2ValueType.UInt64
            ? Convert.ToUInt32(ReadNumericUInt64(row, field))
            : Convert.ToUInt32(ReadNumericInt64(row, field));
    }

    public static long ReadInt64(Wdc5Row row, Db2FieldAccessor accessor)
    {
        var field = accessor.Field;

        if (field.IsVirtual)
            return GetVirtualNumeric(row, field);

        return field.ValueType switch
        {
            Db2ValueType.Single => Convert.ToInt64(row.GetScalar<float>(field.ColumnStartIndex)),
            Db2ValueType.UInt64 => unchecked((long)row.GetScalar<ulong>(field.ColumnStartIndex)),
            _ => row.GetScalar<long>(field.ColumnStartIndex),
        };
    }

    public static ulong ReadUInt64(Wdc5Row row, Db2FieldAccessor accessor)
    {
        var field = accessor.Field;

        if (field.IsVirtual)
            return GetVirtualNumericUInt32(row, field);

        return field.ValueType switch
        {
            Db2ValueType.Single => Convert.ToUInt64(row.GetScalar<float>(field.ColumnStartIndex)),
            Db2ValueType.Int64 => unchecked((ulong)row.GetScalar<long>(field.ColumnStartIndex)),
            _ => row.GetScalar<ulong>(field.ColumnStartIndex),
        };
    }

    public static T[] ReadArray<T>(Wdc5Row row, Db2FieldAccessor accessor) where T : unmanaged
    {
        var field = accessor.Field;
        if (field.IsVirtual)
            throw new NotSupportedException($"Virtual field '{field.Name}' cannot be materialized as an array.");

        return row.GetArray<T>(field.ColumnStartIndex);
    }

    private static long ReadNumericInt64(Wdc5Row row, Db2FieldSchema field)
    {
        if (field.IsVirtual)
            return GetVirtualNumeric(row, field);

        return field.ValueType switch
        {
            Db2ValueType.Single => Convert.ToInt64(row.GetScalar<float>(field.ColumnStartIndex)),
            Db2ValueType.UInt64 => unchecked((long)row.GetScalar<ulong>(field.ColumnStartIndex)),
            _ => row.GetScalar<long>(field.ColumnStartIndex),
        };
    }

    private static ulong ReadNumericUInt64(Wdc5Row row, Db2FieldSchema field)
    {
        if (field.IsVirtual)
            return GetVirtualNumericUInt32(row, field);

        return field.ValueType switch
        {
            Db2ValueType.Single => Convert.ToUInt64(row.GetScalar<float>(field.ColumnStartIndex)),
            Db2ValueType.Int64 => unchecked((ulong)row.GetScalar<long>(field.ColumnStartIndex)),
            _ => row.GetScalar<ulong>(field.ColumnStartIndex),
        };
    }

    private static double ReadNumericInt64OrUInt64(Wdc5Row row, Db2FieldSchema field)
        => field.ValueType == Db2ValueType.UInt64
            ? ReadNumericUInt64(row, field)
            : ReadNumericInt64(row, field);

    private static int GetVirtualNumeric(Wdc5Row row, Db2FieldSchema field)
    {
        if (!field.IsVirtual)
            throw new InvalidOperationException("Expected a virtual field.");

        if (field.IsId)
            return row.Id;

        if (field.IsRelation)
            return row.ReferenceId;

        throw new NotSupportedException($"Unsupported virtual field '{field.Name}'.");
    }

    private static uint GetVirtualNumericUInt32(Wdc5Row row, Db2FieldSchema field)
    {
        if (!field.IsVirtual)
            throw new InvalidOperationException("Expected a virtual field.");

        if (field.IsId)
            return unchecked((uint)row.Id);

        if (field.IsRelation)
            return unchecked((uint)row.ReferenceId);

        throw new NotSupportedException($"Unsupported virtual field '{field.Name}'.");
    }
}
