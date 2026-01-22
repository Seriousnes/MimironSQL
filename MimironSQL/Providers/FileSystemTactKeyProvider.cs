using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;

namespace MimironSQL.Providers;

public sealed class FileSystemTactKeyProvider(FileSystemTactKeyProviderOptions options) : ITactKeyProvider
{
    private readonly Dictionary<ulong, ReadOnlyMemory<byte>> _keys = File.ReadAllLines(options.KeyFilePath)
            .Select(x => x.Split(' '))
            .Select(x => (Lookup: ulong.Parse(x[0], NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture), KeyValue: Convert.FromHexString(x[1])))
            .ToDictionary(k => k.Lookup, v => new ReadOnlyMemory<byte>(v.KeyValue));

    public bool TryGetKey(ulong tactKeyLookup, out ReadOnlyMemory<byte> key)
    {
        return _keys.TryGetValue(tactKeyLookup, out key);
    }
}
