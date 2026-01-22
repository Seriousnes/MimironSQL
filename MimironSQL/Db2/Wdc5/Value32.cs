using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace MimironSQL.Db2.Wdc5;

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 4)]
public readonly struct Value32
{
    private readonly uint _raw;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public T GetValue<T>() where T : unmanaged
    {
        return Unsafe.As<uint, T>(ref Unsafe.AsRef(in _raw));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Value32 From<T>(T value) where T : unmanaged
    {
        var raw = Unsafe.As<T, uint>(ref value);
        return Unsafe.As<uint, Value32>(ref raw);
    }
}
