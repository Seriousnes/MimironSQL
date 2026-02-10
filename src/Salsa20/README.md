# Salsa20

A lightweight Salsa20 stream cipher implementation for .NET.

## Overview

This library provides a pure .NET implementation of the [Salsa20](https://cr.yp.to/snuffle.html) stream cipher. It is used by MimironSQL to decrypt TACT-encrypted DB2 sections and is typically consumed as a transitive dependency via `MimironSQL.Formats.Wdc5`. It has no external dependencies.

## Installation

```shell
dotnet add package Salsa20
```

In most cases you do not need to install this package directly — it is pulled in automatically when you reference `MimironSQL.Formats.Wdc5`.

## Usage

```csharp
using Security.Cryptography;

byte[] key   = /* 16 or 32 bytes */;
byte[] nonce = /* 8 bytes */;

using var cipher = new Salsa20(key, nonce);
cipher.Transform(ciphertext, plaintext);
```

`Transform` is symmetric — the same call encrypts or decrypts.

## API Reference

```csharp
namespace Security.Cryptography;

public sealed class Salsa20 : IDisposable
{
    // key: 16 or 32 bytes, nonce: 8 bytes
    public Salsa20(ReadOnlySpan<byte> key, ReadOnlySpan<byte> nonce);

    public void Transform(ReadOnlySpan<byte> source, Span<byte> destination);

    public void Dispose();
}
```

## Security Notes

- **Key length** must be 16 or 32 bytes; any other size throws `ArgumentException`.
- **Nonce length** must be exactly 8 bytes.
- **Dispose** zeroes all key material from memory. Always wrap usage in a `using` statement.

## License

[MIT](../../LICENSE.txt)
