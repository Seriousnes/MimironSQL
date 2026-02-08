# Salsa20

Salsa20 stream cipher for decrypting DB2 files.

## Usage

```csharp
var cipher = new Salsa20(key, nonce);  // 16-byte key, 8-byte nonce

// Encrypt/decrypt
byte[] data = ...;
cipher.Transform(data);  // In-place

// Or separate buffers
cipher.Transform(input, output);
```

Used by WDC5 format reader for encrypted sections.

Target: .NET 10.0
