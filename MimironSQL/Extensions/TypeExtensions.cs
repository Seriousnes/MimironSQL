namespace MimironSQL.Extensions;

internal static class TypeExtensions
{
    public static bool IsScalarType(this Type type)
    {
        var underlying = type.UnwrapNullable();

        if (underlying.IsEnum)
            return true;

        return underlying == typeof(bool)
               || underlying == typeof(byte)
               || underlying == typeof(sbyte)
               || underlying == typeof(short)
               || underlying == typeof(ushort)
               || underlying == typeof(int)
               || underlying == typeof(uint)
               || underlying == typeof(long)
               || underlying == typeof(ulong)
               || underlying == typeof(float)
               || underlying == typeof(double);
    }

    public static bool IsNullable(this Type type)
        => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);

    public static Type UnwrapNullable(this Type type)
        => type.IsNullable() ? Nullable.GetUnderlyingType(type)! : type;
}
