using System.Buffers.Binary;
using System.Diagnostics;

namespace MimironSQL.Providers;

[DebuggerDisplay("{ToString()}")]
/// <summary>
/// Represents a 16-byte CASC content or encoding key.
/// </summary>
internal readonly struct CascKey : IEquatable<CascKey>
{
    /// <summary>
    /// The key length in bytes.
    /// </summary>
    public const int Length = 16;

    private readonly ulong _lo;
    private readonly ulong _hi;

    /// <summary>
    /// Initializes a new <see cref="CascKey"/> from a 16-byte buffer.
    /// </summary>
    /// <param name="bytes">The key bytes.</param>
    public CascKey(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != Length)
            throw new ArgumentException($"Key must be {Length} bytes", nameof(bytes));

        _lo = BinaryPrimitives.ReadUInt64LittleEndian(bytes);
        _hi = BinaryPrimitives.ReadUInt64LittleEndian(bytes[8..]);
    }

    /// <summary>
    /// Returns the key as a new byte array.
    /// </summary>
    /// <returns>The key bytes.</returns>
    public byte[] ToByteArray()
    {
        var tmp = new byte[Length];
        CopyTo(tmp);
        return tmp;
    }

    /// <summary>
    /// Copies the key bytes into the provided destination span.
    /// </summary>
    /// <param name="destination">The destination span.</param>
    public void CopyTo(Span<byte> destination)
    {
        if (destination.Length < Length) throw new ArgumentException("Destination too small", nameof(destination));
        BinaryPrimitives.WriteUInt64LittleEndian(destination, _lo);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[8..], _hi);
    }

    /// <summary>
    /// Returns the lowercase hexadecimal string representation of this key.
    /// </summary>
    /// <returns>A lowercase hexadecimal string.</returns>
    public override string ToString()
    {
        Span<byte> bytes = stackalloc byte[Length];
        CopyTo(bytes);
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }

    /// <summary>
    /// Parses a 32-hex-character string into a <see cref="CascKey"/>.
    /// </summary>
    /// <param name="hex">The hexadecimal string.</param>
    /// <returns>The parsed key.</returns>
    public static CascKey ParseHex(string hex)
    {
        ArgumentNullException.ThrowIfNull(hex);
        var bytes = Convert.FromHexString(hex);
        return new CascKey(bytes);
    }

    /// <summary>
    /// Determines whether this key equals another key.
    /// </summary>
    /// <param name="other">The other key.</param>
    /// <returns><see langword="true"/> if the keys are equal; otherwise <see langword="false"/>.</returns>
    public bool Equals(CascKey other) => _lo == other._lo && _hi == other._hi;

    /// <summary>
    /// Determines whether this key equals another object.
    /// </summary>
    /// <param name="obj">The other object.</param>
    /// <returns><see langword="true"/> if the object is an equal key; otherwise <see langword="false"/>.</returns>
    public override bool Equals(object? obj) => obj is CascKey other && Equals(other);

    /// <summary>
    /// Returns a hash code for this key.
    /// </summary>
    /// <returns>A hash code.</returns>
    public override int GetHashCode() => HashCode.Combine(_lo, _hi);

    /// <summary>
    /// Compares two keys for equality.
    /// </summary>
    /// <param name="left">The left key.</param>
    /// <param name="right">The right key.</param>
    /// <returns><see langword="true"/> if the keys are equal; otherwise <see langword="false"/>.</returns>
    public static bool operator ==(CascKey left, CascKey right) => left.Equals(right);

    /// <summary>
    /// Compares two keys for inequality.
    /// </summary>
    /// <param name="left">The left key.</param>
    /// <param name="right">The right key.</param>
    /// <returns><see langword="true"/> if the keys are not equal; otherwise <see langword="false"/>.</returns>
    public static bool operator !=(CascKey left, CascKey right) => !left.Equals(right);
}
