# Salsa20

Salsa20 stream cipher implementation used by MimironSQL to decrypt TACT-encrypted DB2 sections.

## Usage

```csharp
using var cipher = new Salsa20(key, nonce);
cipher.Transform(ciphertext, plaintext);
```

`Transform` is symmetric — the same call encrypts or decrypts.

## Public API

```csharp
public sealed class Salsa20 : IDisposable
{
    // key: 16 or 32 bytes, nonce: 8 bytes
    public Salsa20(ReadOnlySpan<byte> key, ReadOnlySpan<byte> nonce);

    public void Transform(ReadOnlySpan<byte> source, Span<byte> destination);

    // Zeroes key material
    public void Dispose();
}
```
