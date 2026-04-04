using System.Globalization;
using System.Runtime.CompilerServices;

using MimironSQL.Db2;

namespace MimironSQL.Formats.Wdc5.Index;

/// <summary>
/// Encodes typed scalar values as sortable unsigned 64-bit integers for B+-tree storage.
/// The unsigned ordering of encoded values matches the signed/float ordering of the originals.
/// </summary>
internal static class Db2IndexValueEncoder
{
    /// <summary>
    /// Encodes a value to a sortable <see cref="ulong"/>. Supports all integer widths and floats.
    /// </summary>
    public static ulong Encode<T>(T value) where T : unmanaged
    {
        if (typeof(T) == typeof(int))
        {
            return (ulong)unchecked((uint)(Unsafe.As<T, int>(ref value) ^ int.MinValue));
        }

        if (typeof(T) == typeof(uint))
        {
            return Unsafe.As<T, uint>(ref value);
        }

        if (typeof(T) == typeof(long))
        {
            return unchecked((ulong)Unsafe.As<T, long>(ref value)) ^ 0x8000000000000000UL;
        }

        if (typeof(T) == typeof(ulong))
        {
            return Unsafe.As<T, ulong>(ref value);
        }

        if (typeof(T) == typeof(short))
        {
            // Reinterpret bits as ushort then XOR the sign bit — avoids sign-extension problems.
            var raw = Unsafe.As<T, ushort>(ref value);
            return (ulong)(ushort)(raw ^ 0x8000u);
        }

        if (typeof(T) == typeof(ushort))
        {
            return Unsafe.As<T, ushort>(ref value);
        }

        if (typeof(T) == typeof(sbyte))
        {
            var raw = Unsafe.As<T, byte>(ref value);
            return (ulong)(byte)(raw ^ 0x80u);
        }

        if (typeof(T) == typeof(byte))
        {
            return Unsafe.As<T, byte>(ref value);
        }

        if (typeof(T) == typeof(float))
        {
            // IEEE 754 sortable encoding:
            //   negative (sign bit set)  →  ~bits  (maps to [0, 0x7FFFFFFF])
            //   positive (sign bit clear) → bits ^ 0x80000000 (maps to [0x80000000, 0xFFFFFFFF])
            var rawBits = Unsafe.As<T, int>(ref value);
            return rawBits < 0
                ? (ulong)unchecked((uint)~rawBits)
                : (ulong)unchecked((uint)(rawBits ^ int.MinValue));
        }

        if (typeof(T) == typeof(double))
        {
            var rawBits = Unsafe.As<T, long>(ref value);
            return rawBits < 0
                ? (ulong)~rawBits
                : unchecked((ulong)rawBits) ^ 0x8000000000000000UL;
        }

        throw new NotSupportedException($"Encoding not supported for type {typeof(T).FullName}.");
    }

    public static ulong EncodeObject(object value, Db2ValueType valueType, byte byteWidth)
    {
        ArgumentNullException.ThrowIfNull(value);

        return (valueType, byteWidth) switch
        {
            (Db2ValueType.Single, _) => Encode(Convert.ToSingle(value, CultureInfo.InvariantCulture)),
            (Db2ValueType.Int64, 1) => Encode(Convert.ToSByte(value, CultureInfo.InvariantCulture)),
            (Db2ValueType.Int64, 2) => Encode(Convert.ToInt16(value, CultureInfo.InvariantCulture)),
            (Db2ValueType.Int64, 4) => Encode(Convert.ToInt32(value, CultureInfo.InvariantCulture)),
            (Db2ValueType.Int64, 8) => Encode(Convert.ToInt64(value, CultureInfo.InvariantCulture)),
            (Db2ValueType.UInt64, 1) => Encode(Convert.ToByte(value, CultureInfo.InvariantCulture)),
            (Db2ValueType.UInt64, 2) => Encode(Convert.ToUInt16(value, CultureInfo.InvariantCulture)),
            (Db2ValueType.UInt64, 4) => Encode(Convert.ToUInt32(value, CultureInfo.InvariantCulture)),
            (Db2ValueType.UInt64, 8) => Encode(Convert.ToUInt64(value, CultureInfo.InvariantCulture)),
            _ => throw new NotSupportedException($"Encoding not supported for DB2 value type {valueType} with byte width {byteWidth}.")
        };
    }

    /// <summary>
    /// Decodes a sortable <see cref="ulong"/> back to the original typed value.
    /// </summary>
    public static T Decode<T>(ulong encoded) where T : unmanaged
    {
        if (typeof(T) == typeof(int))
        {
            var v = unchecked((int)(uint)encoded ^ int.MinValue);
            return Unsafe.As<int, T>(ref v);
        }

        if (typeof(T) == typeof(uint))
        {
            var v = (uint)encoded;
            return Unsafe.As<uint, T>(ref v);
        }

        if (typeof(T) == typeof(long))
        {
            var v = unchecked((long)(encoded ^ 0x8000000000000000UL));
            return Unsafe.As<long, T>(ref v);
        }

        if (typeof(T) == typeof(ulong))
        {
            return Unsafe.As<ulong, T>(ref encoded);
        }

        if (typeof(T) == typeof(short))
        {
            var v = (ushort)((ushort)encoded ^ 0x8000u);
            return Unsafe.As<ushort, T>(ref v);
        }

        if (typeof(T) == typeof(ushort))
        {
            var v = (ushort)encoded;
            return Unsafe.As<ushort, T>(ref v);
        }

        if (typeof(T) == typeof(sbyte))
        {
            var v = (byte)((byte)encoded ^ 0x80u);
            return Unsafe.As<byte, T>(ref v);
        }

        if (typeof(T) == typeof(byte))
        {
            var v = (byte)encoded;
            return Unsafe.As<byte, T>(ref v);
        }

        if (typeof(T) == typeof(float))
        {
            var e = (uint)encoded;
            // Inverse of the encoding above.
            var rawBits = (e & 0x80000000u) == 0u
                ? ~unchecked((int)e)          // was negative float
                : unchecked((int)e ^ int.MinValue);  // was positive float
            return Unsafe.As<int, T>(ref rawBits);
        }

        if (typeof(T) == typeof(double))
        {
            var rawBits = (encoded & 0x8000000000000000UL) == 0UL
                ? ~unchecked((long)encoded)
                : unchecked((long)(encoded ^ 0x8000000000000000UL));
            return Unsafe.As<long, T>(ref rawBits);
        }

        throw new NotSupportedException($"Decoding not supported for type {typeof(T).FullName}.");
    }
}
