using System.Buffers.Binary;
using System.Numerics;

namespace Security.Cryptography;

/// <summary>
/// A zero-allocation, high-performance implementation of the Salsa20 stream cipher.
/// </summary>
public sealed class Salsa20 : IDisposable
{
    private const int StateSizeInts = 16;
    private const int BlockSizeBytes = 64;

    // The internal state: 16 uints (64 bytes)
    // Layout: Constants, Key, Nonce, Stream Position
    private readonly uint[] _state = new uint[StateSizeInts];

    // "expand 32-byte k"
    private static ReadOnlySpan<uint> Sigma => [0x61707865, 0x3320646E, 0x79622D32, 0x6B206574];
    // "expand 16-byte k"
    private static ReadOnlySpan<uint> Tau => [0x61707865, 0x3120646E, 0x79622D36, 0x6B206574];

    /// <summary>
    /// Initializes the Salsa20 state using a Key and Nonce.
    /// </summary>
    /// <param name="key">Must be 16 or 32 bytes.</param>
    /// <param name="nonce">Must be 8 bytes.</param>
    public Salsa20(ReadOnlySpan<byte> key, ReadOnlySpan<byte> nonce)
    {
        if (nonce.Length != 8)
            throw new ArgumentException("Nonce must be 8 bytes", nameof(nonce));

        // Initialize based on key size (16 or 32 bytes)
        if (key.Length == 32)
        {
            _state[0] = Sigma[0];
            _state[1] = BinaryPrimitives.ReadUInt32LittleEndian(key[..4]);
            _state[2] = BinaryPrimitives.ReadUInt32LittleEndian(key[4..8]);
            _state[3] = BinaryPrimitives.ReadUInt32LittleEndian(key[8..12]);
            _state[4] = BinaryPrimitives.ReadUInt32LittleEndian(key[12..16]);
            _state[5] = Sigma[1];

            _state[10] = Sigma[2];
            _state[11] = BinaryPrimitives.ReadUInt32LittleEndian(key[16..20]);
            _state[12] = BinaryPrimitives.ReadUInt32LittleEndian(key[20..24]);
            _state[13] = BinaryPrimitives.ReadUInt32LittleEndian(key[24..28]);
            _state[14] = BinaryPrimitives.ReadUInt32LittleEndian(key[28..32]);
            _state[15] = Sigma[3];
        }
        else if (key.Length == 16)
        {
            _state[0] = Tau[0];
            _state[1] = BinaryPrimitives.ReadUInt32LittleEndian(key[..4]);
            _state[2] = BinaryPrimitives.ReadUInt32LittleEndian(key[4..8]);
            _state[3] = BinaryPrimitives.ReadUInt32LittleEndian(key[8..12]);
            _state[4] = BinaryPrimitives.ReadUInt32LittleEndian(key[12..]);
            _state[5] = Tau[1];

            _state[10] = Tau[2];
            // Repeat key for 16-byte version
            _state[11] = _state[1];
            _state[12] = _state[2];
            _state[13] = _state[3];
            _state[14] = _state[4];
            _state[15] = Tau[3];
        }
        else
        {
            throw new ArgumentException("Key must be 16 or 32 bytes", nameof(key));
        }

        // Set Nonce
        _state[6] = BinaryPrimitives.ReadUInt32LittleEndian(nonce[..4]);
        _state[7] = BinaryPrimitives.ReadUInt32LittleEndian(nonce[4..]);

        // Set Block Counter to 0
        _state[8] = 0;
        _state[9] = 0;
    }

    /// <summary>
    /// Encrypts or decrypts the buffer. (Symmetric operation).
    /// </summary>
    /// <param name="source">The input data.</param>
    /// <param name="destination">The output buffer. Must be at least as large as source.</param>
    public void Transform(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        if (destination.Length < source.Length)
            throw new ArgumentException("Destination buffer is too small.", nameof(destination));

        // Use stackalloc to avoid GC pressure for the keystream block
        Span<byte> keyStreamBlock = stackalloc byte[BlockSizeBytes];

        int offset = 0;
        int remaining = source.Length;

        while (remaining > 0)
        {
            // Generate next 64 bytes of keystream
            Core(_state, keyStreamBlock);

            // Determine how many bytes to process in this iteration
            int count = Math.Min(remaining, BlockSizeBytes);

            // XOR source with keystream -> destination
            Xor(source.Slice(offset, count), keyStreamBlock.Slice(0, count), destination.Slice(offset, count));

            // Increment 64-bit counter in the state
            IncrementCounter();

            remaining -= count;
            offset += count;
        }
    }

    private void IncrementCounter()
    {
        unchecked
        {
            // Low 32-bits
            _state[8]++;
            // If overflow, increment High 32-bits
            if (_state[8] == 0)
            {
                _state[9]++;
            }
        }
    }

    // Use Hardware Intrinsics for XOR operations where possible
    private static void Xor(ReadOnlySpan<byte> input, ReadOnlySpan<byte> key, Span<byte> output)
    {
        int i = 0;
        // Vectorized loop can be added here using Vector<T> or Vector128<T>
        // The JIT is generally very good at unrolling this specific simple loop.
        for (; i < input.Length; i++)
        {
            output[i] = (byte)(input[i] ^ key[i]);
        }
    }

    /// <summary>
    /// The Salsa20 Core function: Hash(State) -> Keystream Block
    /// </summary>
    private static void Core(ReadOnlySpan<uint> inputState, Span<byte> output)
    {
        // Copy state to working buffer
        Span<uint> x = stackalloc uint[StateSizeInts];
        inputState.CopyTo(x);

        // 20 Rounds (10 iterations of double-rounds)
        for (int i = 0; i < 10; i++)
        {
            // Column round
            x[4] ^= BitOperations.RotateLeft(x[0] + x[12], 7);
            x[8] ^= BitOperations.RotateLeft(x[4] + x[0], 9);
            x[12] ^= BitOperations.RotateLeft(x[8] + x[4], 13);
            x[0] ^= BitOperations.RotateLeft(x[12] + x[8], 18);

            x[9] ^= BitOperations.RotateLeft(x[5] + x[1], 7);
            x[13] ^= BitOperations.RotateLeft(x[9] + x[5], 9);
            x[1] ^= BitOperations.RotateLeft(x[13] + x[9], 13);
            x[5] ^= BitOperations.RotateLeft(x[1] + x[13], 18);

            x[14] ^= BitOperations.RotateLeft(x[10] + x[6], 7);
            x[2] ^= BitOperations.RotateLeft(x[14] + x[10], 9);
            x[6] ^= BitOperations.RotateLeft(x[2] + x[14], 13);
            x[10] ^= BitOperations.RotateLeft(x[6] + x[2], 18);

            x[3] ^= BitOperations.RotateLeft(x[15] + x[11], 7);
            x[7] ^= BitOperations.RotateLeft(x[3] + x[15], 9);
            x[11] ^= BitOperations.RotateLeft(x[7] + x[3], 13);
            x[15] ^= BitOperations.RotateLeft(x[11] + x[7], 18);

            // Row round
            x[1] ^= BitOperations.RotateLeft(x[0] + x[3], 7);
            x[2] ^= BitOperations.RotateLeft(x[1] + x[0], 9);
            x[3] ^= BitOperations.RotateLeft(x[2] + x[1], 13);
            x[0] ^= BitOperations.RotateLeft(x[3] + x[2], 18);

            x[6] ^= BitOperations.RotateLeft(x[5] + x[4], 7);
            x[7] ^= BitOperations.RotateLeft(x[6] + x[5], 9);
            x[4] ^= BitOperations.RotateLeft(x[7] + x[6], 13);
            x[5] ^= BitOperations.RotateLeft(x[4] + x[7], 18);

            x[11] ^= BitOperations.RotateLeft(x[10] + x[9], 7);
            x[8] ^= BitOperations.RotateLeft(x[11] + x[10], 9);
            x[9] ^= BitOperations.RotateLeft(x[8] + x[11], 13);
            x[10] ^= BitOperations.RotateLeft(x[9] + x[8], 18);

            x[12] ^= BitOperations.RotateLeft(x[15] + x[14], 7);
            x[13] ^= BitOperations.RotateLeft(x[12] + x[15], 9);
            x[14] ^= BitOperations.RotateLeft(x[13] + x[12], 13);
            x[15] ^= BitOperations.RotateLeft(x[14] + x[13], 18);
        }

        // Add the original state to the result and write to byte output
        for (int i = 0; i < StateSizeInts; i++)
        {
            uint val = x[i] + inputState[i];
            BinaryPrimitives.WriteUInt32LittleEndian(output.Slice(i * 4), val);
        }
    }

    public void Dispose()
    {
        // Zero out key material in memory
        Array.Clear(_state);
    }
}