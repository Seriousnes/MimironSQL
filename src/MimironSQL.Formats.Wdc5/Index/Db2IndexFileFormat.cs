using System.Buffers.Binary;
using System.Text;

namespace MimironSQL.Formats.Wdc5.Index;

internal static class Db2IndexFileFormat
{
    public const int PageSize = 4096;
    public const uint Magic = 0x49324244u;
    public const int FormatVersion = 1;
    public const byte PageTypeInternal = 0x01;
    public const byte PageTypeLeaf = 0x02;
    public const int WowVersionMaxBytes = 32;
    public const int TableNameMaxBytes = 32;
    public const long EmptyRootOffset = -1L;

    public const int HdrMagicOffset = 0;
    public const int HdrVersionOffset = 4;
    public const int HdrWowVersionOffset = 8;
    public const int HdrLayoutHashOffset = 40;
    public const int HdrTableNameOffset = 44;
    public const int HdrFieldIndexOffset = 76;
    public const int HdrValueTypeOffset = 80;
    public const int HdrValueByteWidthOffset = 81;
    public const int HdrPageSizeOffset = 84;
    public const int HdrRootPageOffsetOffset = 88;
    public const int HdrRecordCountOffset = 96;
    public const int HdrTreeHeightOffset = 100;

    public const int LeafTypeOffset = 0;
    public const int LeafEntryCountOffset = 1;
    public const int LeafNextPageOffset = 3;
    public const int LeafEntriesOffset = 11;

    public static int GetEntrySize(byte valueByteWidth) => valueByteWidth + 6;

    public static int GetMaxLeafEntries(byte valueByteWidth)
        => (PageSize - LeafEntriesOffset) / GetEntrySize(valueByteWidth);

    public const int InternalTypeOffset = 0;
    public const int InternalKeyCountOffset = 1;
    public const int InternalKeysOffset = 3;

    public static int GetMaxInternalKeys(byte valueByteWidth)
        => (PageSize - 11) / (valueByteWidth + 8);

    public static uint ReadUInt32(byte[] buf, int offset)
        => BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(offset));

    public static int ReadInt32(byte[] buf, int offset)
        => BinaryPrimitives.ReadInt32LittleEndian(buf.AsSpan(offset));

    public static long ReadInt64(byte[] buf, int offset)
        => BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(offset));

    public static ushort ReadUInt16(byte[] buf, int offset)
        => BinaryPrimitives.ReadUInt16LittleEndian(buf.AsSpan(offset));

    public static ulong ReadKey(byte[] buf, int offset, byte byteWidth) => byteWidth switch
    {
        1 => buf[offset],
        2 => BinaryPrimitives.ReadUInt16LittleEndian(buf.AsSpan(offset)),
        4 => BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(offset)),
        8 => BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(offset)),
        _ => throw new ArgumentOutOfRangeException(nameof(byteWidth)),
    };

    public static void WriteUInt32(byte[] buf, int offset, uint value)
        => BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(offset), value);

    public static void WriteInt32(byte[] buf, int offset, int value)
        => BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(offset), value);

    public static void WriteInt64(byte[] buf, int offset, long value)
        => BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(offset), value);

    public static void WriteUInt16(byte[] buf, int offset, ushort value)
        => BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(offset), value);

    public static void WriteKey(byte[] buf, int offset, ulong value, byte byteWidth)
    {
        switch (byteWidth)
        {
            case 1: buf[offset] = (byte)(value & 0xFFu); break;
            case 2: BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(offset), (ushort)(value & 0xFFFFu)); break;
            case 4: BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(offset), (uint)(value & 0xFFFFFFFFu)); break;
            case 8: BinaryPrimitives.WriteUInt64LittleEndian(buf.AsSpan(offset), value); break;
            default: throw new ArgumentOutOfRangeException(nameof(byteWidth));
        }
    }

    public static void WriteFixedLengthString(byte[] buf, int offset, int maxBytes, string? value)
    {
        var span = buf.AsSpan(offset, maxBytes);
        span.Clear();
        if (string.IsNullOrEmpty(value))
        {
            return;
        }

        var bytes = Encoding.UTF8.GetBytes(value);
        bytes.AsSpan(0, Math.Min(bytes.Length, maxBytes - 1)).CopyTo(span);
    }

    public static string ReadFixedLengthString(byte[] buf, int offset, int maxBytes)
    {
        var span = buf.AsSpan(offset, maxBytes);
        var end = span.IndexOf((byte)0);
        if (end < 0)
        {
            end = maxBytes;
        }

        return end == 0 ? string.Empty : Encoding.UTF8.GetString(span[..end]);
    }
}
