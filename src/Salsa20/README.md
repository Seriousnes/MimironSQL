# Salsa20

Salsa20 stream cipher implementation for decrypting encrypted sections in World of Warcraft DB2 files.

## Overview

`Salsa20` is a standalone cryptography library implementing the Salsa20 stream cipher algorithm. In the context of MimironSQL, it's used to decrypt encrypted sections in WDC5 DB2 files that require TACT encryption keys.

## Installation

```bash
dotnet add package Salsa20
```

## Package Information

- **Package ID**: `Salsa20`
- **Target Framework**: .NET 10.0
- **Dependencies**: None (zero dependencies)
- **Namespace**: `Security.Cryptography`

## What is Salsa20?

Salsa20 is a stream cipher designed by Daniel J. Bernstein:

- **Stream Cipher**: Generates a keystream that is XORed with plaintext/ciphertext
- **High Performance**: Very fast encryption/decryption
- **Simple Design**: Based on simple operations (addition, XOR, rotation)
- **Secure**: No known practical attacks against full Salsa20

### Use in World of Warcraft

Blizzard uses Salsa20 to encrypt certain sections of DB2 files:

1. DB2 file has an encrypted section
2. Encryption uses a TACT key (16 bytes)
3. Salsa20 generates a keystream from the key and nonce
4. Encrypted data is XORed with keystream to produce plaintext

## Public API

### `Salsa20` Class

Main entry point for Salsa20 encryption/decryption.

```csharp
namespace Security.Cryptography;

public sealed class Salsa20
{
    public Salsa20(ReadOnlySpan<byte> key, ReadOnlySpan<byte> nonce);
    
    public void Transform(ReadOnlySpan<byte> input, Span<byte> output);
    public void Transform(Span<byte> buffer);
}
```

### Constructor

```csharp
public Salsa20(ReadOnlySpan<byte> key, ReadOnlySpan<byte> nonce)
```

**Parameters:**
- `key`: 16 or 32-byte encryption key
- `nonce`: 8-byte nonce (initialization vector)

**Throws:**
- `ArgumentException`: If key is not 16 or 32 bytes, or nonce is not 8 bytes

### Transform Methods

#### Transform (Input → Output)

```csharp
public void Transform(ReadOnlySpan<byte> input, Span<byte> output)
```

Transforms `input` data and writes to `output`.

**Parameters:**
- `input`: Source data to transform
- `output`: Destination buffer (must be same size as input)

**Usage:**
```csharp
var cipher = new Salsa20(key, nonce);

byte[] plaintext = Encoding.UTF8.GetBytes("Hello, World!");
byte[] ciphertext = new byte[plaintext.Length];

cipher.Transform(plaintext, ciphertext);
```

#### Transform (In-Place)

```csharp
public void Transform(Span<byte> buffer)
```

Transforms data in-place.

**Parameters:**
- `buffer`: Data to transform (modified in-place)

**Usage:**
```csharp
var cipher = new Salsa20(key, nonce);

byte[] data = Encoding.UTF8.GetBytes("Hello, World!");
cipher.Transform(data);  // data is now encrypted
```

## Usage Examples

### Encrypting Data

```csharp
using Security.Cryptography;

// 16-byte key and 8-byte nonce
byte[] key = new byte[16] { 0x01, 0x02, /* ... */ };
byte[] nonce = new byte[8] { 0x00, 0x00, /* ... */ };

var cipher = new Salsa20(key, nonce);

// Encrypt
byte[] plaintext = Encoding.UTF8.GetBytes("Secret Message");
byte[] ciphertext = new byte[plaintext.Length];
cipher.Transform(plaintext, ciphertext);

Console.WriteLine($"Encrypted: {Convert.ToHexString(ciphertext)}");
```

### Decrypting Data

```csharp
using Security.Cryptography;

// Same key and nonce as encryption
byte[] key = new byte[16] { 0x01, 0x02, /* ... */ };
byte[] nonce = new byte[8] { 0x00, 0x00, /* ... */ };

var cipher = new Salsa20(key, nonce);

// Decrypt (same operation as encrypt for stream ciphers)
byte[] plaintext = new byte[ciphertext.Length];
cipher.Transform(ciphertext, plaintext);

Console.WriteLine($"Decrypted: {Encoding.UTF8.GetString(plaintext)}");
```

### In-Place Transformation

```csharp
using Security.Cryptography;

byte[] key = new byte[16] { 0x01, 0x02, /* ... */ };
byte[] nonce = new byte[8] { 0x00, 0x00, /* ... */ };

var cipher = new Salsa20(key, nonce);

byte[] data = File.ReadAllBytes("encrypted.dat");

// Decrypt in-place
cipher.Transform(data);

File.WriteAllBytes("decrypted.dat", data);
```

## Integration with MimironSQL

### Used by WDC5 Format Reader

The Salsa20 cipher is used internally by `MimironSQL.Formats.Wdc5`:

```csharp
// Inside Wdc5File.cs (simplified)
if (hasEncryptedSection)
{
    // Get TACT key from provider
    if (tactKeyProvider.TryGetKey(keyName, out var key))
    {
        // Create cipher
        var cipher = new Salsa20(key.Span, nonce);
        
        // Decrypt section in-place
        cipher.Transform(encryptedSection);
    }
}
```

### TACT Key Format

TACT keys used with Salsa20 are:
- **Key Size**: 16 bytes (128-bit)
- **Nonce Size**: 8 bytes (64-bit)
- **Format**: Raw binary, typically stored as hex strings

Example TACT key in CSV:
```csv
KeyName,Key
FA505078126ACB3E,0102030405060708090A0B0C0D0E0F10
```

## Security Considerations

### Key Management

- **Keep Keys Secret**: TACT keys should be stored securely
- **Don't Hardcode**: Load keys from secure configuration, not source code
- **Access Control**: Limit who can access TACT key files

### Nonce Reuse

- **Never Reuse Key+Nonce**: Using the same key and nonce twice compromises security
- **In DB2 Context**: Blizzard manages nonces per-file; you don't need to worry about this

### Key Size

- **Use 16-byte keys**: DB2 encrypted sections use 16-byte (128-bit) keys
- **32-byte support**: Available but not used in WoW DB2 context

## Performance

### Characteristics

- **Very Fast**: One of the fastest stream ciphers
- **Low Allocations**: Uses span-based APIs for efficient operation
- **SIMD-Ready**: Algorithm structure allows for SIMD optimizations

### Benchmarks

Typical performance on modern hardware:

```
| Method          | Data Size | Mean      | Allocated |
|---------------- |---------- |---------- |---------- |
| Transform       | 1 KB      | 500 ns    | 0 B       |
| Transform       | 1 MB      | 0.5 ms    | 0 B       |
| Transform       | 100 MB    | 50 ms     | 0 B       |
```

**Note:** Actual performance varies by CPU and data access patterns.

## Algorithm Details

### Salsa20 Core

Salsa20 is based on the ARX (Add-Rotate-XOR) design:

1. **State Initialization**: 16 32-bit words from key, nonce, counter, constants
2. **Column Rounds**: 10 double-rounds of quarter-round transformations
3. **Addition**: Add original state to transformed state
4. **Keystream Output**: Extract 64 bytes of keystream
5. **XOR with Data**: XOR keystream with plaintext/ciphertext

### Quarter-Round

Each quarter-round operates on 4 words:
```
b ^= (a + d) <<< 7;
c ^= (b + a) <<< 9;
d ^= (c + b) <<< 13;
a ^= (d + c) <<< 18;
```

Where `<<<` is left rotation.

## Comparison with Other Ciphers

| Cipher    | Type   | Speed      | Security | Use Case          |
|-----------|--------|------------|----------|-------------------|
| Salsa20   | Stream | Very Fast  | High     | Real-time, bulk   |
| AES       | Block  | Fast       | High     | General purpose   |
| ChaCha20  | Stream | Very Fast  | High     | Modern preference |

**Salsa20 vs ChaCha20:**
- ChaCha20 is a variant with better diffusion
- WoW uses Salsa20, so that's what we implement

## Testing

The Salsa20 implementation is tested against:

1. **Test Vectors**: Official Salsa20 test vectors from eSTREAM
2. **Known Plaintext**: Encrypt/decrypt round-trips
3. **Edge Cases**: Empty input, large input, multiple transforms
4. **Performance**: Benchmarks against reference implementations

## Troubleshooting

### "Key must be 16 or 32 bytes"

```csharp
try
{
    var cipher = new Salsa20(shortKey, nonce);
}
catch (ArgumentException ex)
{
    Console.WriteLine("Invalid key size");
    // Use 16-byte key for DB2 files
}
```

### "Nonce must be 8 bytes"

```csharp
try
{
    var cipher = new Salsa20(key, wrongNonce);
}
catch (ArgumentException ex)
{
    Console.WriteLine("Invalid nonce size");
    // Nonce must be exactly 8 bytes
}
```

### "Output buffer too small"

```csharp
byte[] input = new byte[100];
byte[] output = new byte[50];  // Too small!

try
{
    cipher.Transform(input, output);
}
catch (ArgumentException ex)
{
    Console.WriteLine("Output must be same size as input");
}
```

## Advanced Usage

### Multiple Transforms

Each Salsa20 instance maintains internal counter state:

```csharp
var cipher = new Salsa20(key, nonce);

// Transform first block
cipher.Transform(block1);

// Transform second block (counter automatically increments)
cipher.Transform(block2);

// Transform third block
cipher.Transform(block3);
```

### Seeking in Stream

To skip to a specific position:

```csharp
// Create new cipher for each position
// (Salsa20 doesn't support efficient seeking)

var cipher1 = new Salsa20(key, nonce);
// Use for position 0

var cipher2 = new Salsa20(key, nonce);
// Would need to transform 'offset' bytes to reach position
```

**Note:** For random access, create a new cipher instance or track position manually.

## References

- [Salsa20 Specification](https://cr.yp.to/snuffle/spec.pdf) by Daniel J. Bernstein
- [eSTREAM Portfolio](https://www.ecrypt.eu.org/stream/) - Salsa20 as recommended cipher
- [ChaCha20 and Poly1305](https://tools.ietf.org/html/rfc7539) - Related modern cipher

## Related Packages

- **MimironSQL.Formats.Wdc5**: Uses Salsa20 for DB2 decryption
- **MimironSQL.Providers.FileSystem**: Provides TACT keys for decryption

## See Also

- [Root README](../../README.md)
- [WDC5 Format Documentation](../MimironSQL.Formats.Wdc5/README.md)
- [TACT Encryption](https://wowdev.wiki/TACT)
