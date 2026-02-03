namespace MimironSQL.Providers;

public static class CascBucket
{
    // https://wowdev.wiki/CASC
    public static byte GetBucketIndex(ReadOnlySpan<byte> k16)
    {
        if (k16.Length < 9)
            throw new ArgumentException("Key must be at least 9 bytes.", nameof(k16));

        byte i = (byte)(k16[0] ^ k16[1] ^ k16[2] ^ k16[3] ^ k16[4] ^ k16[5] ^ k16[6] ^ k16[7] ^ k16[8]);
        return (byte)((i & 0x0F) ^ (i >> 4));
    }

    public static byte GetBucketIndexCrossReference(ReadOnlySpan<byte> k16)
    {
        var i = GetBucketIndex(k16);
        return (byte)((i + 1) % 16);
    }
}
