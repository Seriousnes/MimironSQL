using Org.BouncyCastle.Crypto.Engines;
using Org.BouncyCastle.Crypto.Parameters;
using MimironSalsa20 = Security.Cryptography.Salsa20;

namespace Salsa20.Tests;

public sealed class Salsa20Tests
{
    [Theory]
    [InlineData(16, 0)]
    [InlineData(16, 1)]
    [InlineData(16, 63)]
    [InlineData(16, 64)]
    [InlineData(16, 65)]
    [InlineData(32, 0)]
    [InlineData(32, 1)]
    [InlineData(32, 63)]
    [InlineData(32, 64)]
    [InlineData(32, 65)]
    public void Transform_MatchesBouncyCastle(int keySizeBytes, int messageLength)
    {
        var key = new byte[keySizeBytes];
        for (int i = 0; i < key.Length; i++)
            key[i] = (byte)i;

        var nonce = new byte[8];
        for (int i = 0; i < nonce.Length; i++)
            nonce[i] = (byte)(0xF0 + i);

        var plaintext = new byte[messageLength];
        for (int i = 0; i < plaintext.Length; i++)
            plaintext[i] = (byte)(0xA5 ^ i);

        var expected = Salsa20WithBouncyCastle(key, nonce, plaintext);
        var actual = Salsa20WithMimiron(key, nonce, plaintext);

        Assert.Equal(expected, actual);
    }

    private static byte[] Salsa20WithMimiron(byte[] key, byte[] nonce, byte[] plaintext)
    {
        var output = new byte[plaintext.Length];

        using var salsa20 = new MimironSalsa20(key, nonce);
        salsa20.Transform(plaintext, output);

        return output;
    }

    private static byte[] Salsa20WithBouncyCastle(byte[] key, byte[] nonce, byte[] plaintext)
    {
        var engine = new Salsa20Engine();
        engine.Init(true, new ParametersWithIV(new KeyParameter(key), nonce));

        var output = new byte[plaintext.Length];
        engine.ProcessBytes(plaintext, 0, plaintext.Length, output, 0);
        return output;
    }
}
