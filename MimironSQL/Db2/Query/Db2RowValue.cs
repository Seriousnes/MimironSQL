using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using System;

namespace MimironSQL.Db2.Query;

internal static class Db2RowValue
{
    public static T Read<T>(Wdc5Row row, Db2FieldAccessor accessor)
    {
        var field = accessor.Field;

        if (typeof(T) == typeof(string))
        {
            if (field.IsVirtual)
                throw new NotSupportedException($"Virtual field '{field.Name}' cannot be materialized as a string.");

            _ = row.TryGetString(field.ColumnStartIndex, out var str);
            return (T)(object)str;
        }

        if (typeof(T) == typeof(float))
        {
            var value = field.IsVirtual
                ? GetVirtualNumeric(row, field)
                : field.ValueType == Db2ValueType.Single
                    ? row.GetScalar<float>(field.ColumnStartIndex)
                    : Convert.ToSingle(ReadNumericRaw(row, field));

            return (T)(object)value;
        }

        if (typeof(T) == typeof(double))
            return (T)(object)Convert.ToDouble(ReadNumericRaw(row, field));

        if (typeof(T) == typeof(bool))
            return (T)(object)(ReadInt64(row, accessor) != 0);

        if (typeof(T) == typeof(byte))
            return (T)(object)Convert.ToByte(ReadNumericRaw(row, field));

        if (typeof(T) == typeof(sbyte))
            return (T)(object)Convert.ToSByte(ReadNumericRaw(row, field));

        if (typeof(T) == typeof(short))
            return (T)(object)Convert.ToInt16(ReadNumericRaw(row, field));

        if (typeof(T) == typeof(ushort))
            return (T)(object)Convert.ToUInt16(ReadNumericRaw(row, field));

        if (typeof(T) == typeof(int))
            return (T)(object)Convert.ToInt32(ReadNumericRaw(row, field));

        if (typeof(T) == typeof(uint))
            return (T)(object)Convert.ToUInt32(ReadNumericRaw(row, field));

        if (typeof(T) == typeof(long))
            return (T)(object)ReadInt64(row, accessor);

        if (typeof(T) == typeof(ulong))
            return (T)(object)ReadUInt64(row, accessor);

        throw new NotSupportedException($"Unsupported target type {typeof(T).FullName} for DB2 field reads.");
    }

    public static T[] ReadArray<T>(Wdc5Row row, Db2FieldAccessor accessor) where T : unmanaged
    {
        var field = accessor.Field;
        if (field.IsVirtual)
            throw new NotSupportedException($"Virtual field '{field.Name}' cannot be materialized as an array.");

        return row.GetArray<T>(field.ColumnStartIndex);
    }

    private static long ReadInt64(Wdc5Row row, Db2FieldAccessor accessor)
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

    private static ulong ReadUInt64(Wdc5Row row, Db2FieldAccessor accessor)
    {
        var field = accessor.Field;

        if (field.IsVirtual)
            return unchecked((ulong)GetVirtualNumeric(row, field));

        return field.ValueType switch
        {
            Db2ValueType.Single => Convert.ToUInt64(row.GetScalar<float>(field.ColumnStartIndex)),
            Db2ValueType.Int64 => unchecked((ulong)row.GetScalar<long>(field.ColumnStartIndex)),
            _ => row.GetScalar<ulong>(field.ColumnStartIndex),
        };
    }

    private static object ReadNumericRaw(Wdc5Row row, Db2FieldSchema field)
    {
        if (field.IsVirtual)
            return GetVirtualNumeric(row, field);

        return field.ValueType switch
        {
            Db2ValueType.Single => row.GetScalar<float>(field.ColumnStartIndex),
            Db2ValueType.UInt64 => row.GetScalar<ulong>(field.ColumnStartIndex),
            _ => row.GetScalar<long>(field.ColumnStartIndex),
        };
    }

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
}
