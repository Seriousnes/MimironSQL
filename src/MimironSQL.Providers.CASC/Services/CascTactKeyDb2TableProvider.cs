using System.Buffers.Binary;

using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;

namespace MimironSQL.Providers;

/// <summary>
/// Resolves TACT keys by reading the <c>TactKey</c> and <c>TactKeyLookup</c> DB2 tables from CASC.
/// </summary>
/// <remarks>
/// This provider is designed to be safe for singleton DI registration.
/// It eagerly preloads the key map during construction.
/// </remarks>
public sealed class CascTactKeyDb2TableProvider : ITactKeyProvider
{
    private readonly Dictionary<ulong, byte[]> _lookupToKey;

    /// <summary>
    /// Creates a provider that preloads TACT keys from CASC DB2 tables.
    /// </summary>
    /// <param name="manifestProvider">The manifest provider used to resolve DB2 content keys.</param>
    /// <param name="options">CASC provider options.</param>
    /// <param name="seedProvider">An optional seed key provider (e.g. from WoW.txt) used to decrypt encrypted content while loading.</param>
    public CascTactKeyDb2TableProvider(
        IManifestProvider manifestProvider,
        CascDb2ProviderOptions options,
        ITactKeyProvider? seedProvider)
    {
        ArgumentNullException.ThrowIfNull(manifestProvider);
        ArgumentNullException.ThrowIfNull(options);

        _lookupToKey = LoadKeys(manifestProvider, options, seedProvider);
    }

    /// <inheritdoc />
    public bool TryGetKey(ulong tactKeyLookup, out ReadOnlyMemory<byte> key)
    {
        if (_lookupToKey.TryGetValue(tactKeyLookup, out var bytes))
        {
            key = bytes;
            return true;
        }

        key = default;
        return false;
    }

    private static Dictionary<ulong, byte[]> LoadKeys(
        IManifestProvider manifestProvider,
        CascDb2ProviderOptions options,
        ITactKeyProvider? seedProvider)
    {
        // Bootstrap: use a stream provider that only knows about the seed keys (to avoid recursion).
        var streamProvider = new CascDb2StreamProvider(manifestProvider, options, seedProvider);
        var format = new Wdc5Format(seedProvider);

        Dictionary<ulong, int> tactKeyIdByLookup;
        try
        {
            tactKeyIdByLookup = ReadTactKeyIdByLookup(format, streamProvider);
        }
        catch (Exception ex) when (ex is FileNotFoundException or ArgumentOutOfRangeException or KeyNotFoundException or InvalidDataException)
        {
            return new Dictionary<ulong, byte[]>();
        }

        Dictionary<int, byte[]> keyByTactKeyId;
        try
        {
            keyByTactKeyId = ReadKeyByTactKeyId(format, streamProvider);
        }
        catch (Exception ex) when (ex is FileNotFoundException or ArgumentOutOfRangeException or KeyNotFoundException or InvalidDataException)
        {
            return new Dictionary<ulong, byte[]>();
        }

        var lookupToKey = new Dictionary<ulong, byte[]>(capacity: tactKeyIdByLookup.Count);

        foreach (var (lookup, tactKeyId) in tactKeyIdByLookup)
        {
            if (keyByTactKeyId.TryGetValue(tactKeyId, out var key))
                lookupToKey[lookup] = key;
        }

        return lookupToKey;
    }

    private static Dictionary<ulong, int> ReadTactKeyIdByLookup(IDb2Format format, IDb2StreamProvider streamProvider)
    {
        using var stream = streamProvider.OpenDb2Stream("TactKeyLookup");
        using var file = format.OpenFile(stream);

        var map = new Dictionary<ulong, int>(capacity: file.RecordsCount);

        foreach (var handle in file.EnumerateRowHandles())
        {
            // Field order is defined by the DBD. For these tables we assume:
            // ID is implicit (handle.RowId), 0: TACTID<u8>[8]
            var tactIdBytes = file.ReadField<byte[]>(handle, fieldIndex: 0);
            if (tactIdBytes.Length != 8)
                continue;

            var lookup = BinaryPrimitives.ReadUInt64LittleEndian(tactIdBytes);
            map[lookup] = handle.RowId;
        }

        return map;
    }

    private static Dictionary<int, byte[]> ReadKeyByTactKeyId(IDb2Format format, IDb2StreamProvider streamProvider)
    {
        using var stream = streamProvider.OpenDb2Stream("TactKey");
        using var file = format.OpenFile(stream);

        var map = new Dictionary<int, byte[]>(capacity: file.RecordsCount);

        foreach (var handle in file.EnumerateRowHandles())
        {
            // Field order is defined by the DBD. For these tables we assume:
            // ID is implicit (handle.RowId), 0: Key<u8>[16]
            var keyBytes = file.ReadField<byte[]>(handle, fieldIndex: 0);
            if (keyBytes.Length != 16)
                continue;

            map[handle.RowId] = keyBytes;
        }

        return map;
    }
}
