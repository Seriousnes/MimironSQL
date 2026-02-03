using System.Buffers.Binary;
using System.Diagnostics;

namespace MimironSQL.Providers;

[DebuggerDisplay("{ToString()}")]
public readonly struct CascKey : IEquatable<CascKey>
{
    public const int Length = 16;

    private readonly ulong _lo;
    private readonly ulong _hi;

    public CascKey(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != Length)
            throw new ArgumentException($"Key must be {Length} bytes", nameof(bytes));

        _lo = BinaryPrimitives.ReadUInt64LittleEndian(bytes);
        _hi = BinaryPrimitives.ReadUInt64LittleEndian(bytes[8..]);
    }

    public byte[] ToByteArray()
    {
        var tmp = new byte[Length];
        CopyTo(tmp);
        return tmp;
    }

    public void CopyTo(Span<byte> destination)
    {
        if (destination.Length < Length) throw new ArgumentException("Destination too small", nameof(destination));
        BinaryPrimitives.WriteUInt64LittleEndian(destination, _lo);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[8..], _hi);
    }

    public override string ToString()
    {
        Span<byte> bytes = stackalloc byte[Length];
        CopyTo(bytes);
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }

    public static CascKey ParseHex(string hex)
    {
        ArgumentNullException.ThrowIfNull(hex);
        var bytes = Convert.FromHexString(hex);
        return new CascKey(bytes);
    }

    public bool Equals(CascKey other) => _lo == other._lo && _hi == other._hi;
    public override bool Equals(object? obj) => obj is CascKey other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(_lo, _hi);

    public static bool operator ==(CascKey left, CascKey right) => left.Equals(right);
    public static bool operator !=(CascKey left, CascKey right) => !left.Equals(right);
}
