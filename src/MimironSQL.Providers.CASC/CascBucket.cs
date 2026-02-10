namespace MimironSQL.Providers;

/// <summary>
/// Helpers for computing CASC bucket indices from hash keys.
/// </summary>
internal static class CascBucket
{
    // https://wowdev.wiki/CASC
    /// <summary>
    /// Computes the CASC bucket index for a 16-byte key.
    /// </summary>
    /// <param name="k16">A 16-byte key.</param>
    /// <returns>The bucket index in the range 0-15.</returns>
    public static byte GetBucketIndex(ReadOnlySpan<byte> k16)
    {
        if (k16.Length < 9)
            throw new ArgumentException("Key must be at least 9 bytes.", nameof(k16));

        byte i = (byte)(k16[0] ^ k16[1] ^ k16[2] ^ k16[3] ^ k16[4] ^ k16[5] ^ k16[6] ^ k16[7] ^ k16[8]);
        return (byte)((i & 0x0F) ^ (i >> 4));
    }

    /// <summary>
    /// Computes the cross-reference bucket index used by CASC implementations.
    /// </summary>
    /// <param name="k16">A 16-byte key.</param>
    /// <returns>The cross-reference bucket index in the range 0-15.</returns>
    public static byte GetBucketIndexCrossReference(ReadOnlySpan<byte> k16)
    {
        var i = GetBucketIndex(k16);
        return (byte)((i + 1) % 16);
    }
}
