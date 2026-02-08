namespace MimironSQL.EntityFrameworkCore.Extensions;

internal static class TypeExtensions
{
    private static readonly HashSet<Type> scalarTypes =
        [
            typeof(bool),
            typeof(byte),
            typeof(sbyte),
            typeof(short),
            typeof(ushort),
            typeof(int),
            typeof(uint),
            typeof(long),
            typeof(ulong),
            typeof(float),
            typeof(double)
        ];

    public static bool IsScalarType(this Type type)
    {
        var underlying = type.UnwrapNullable();

        if (underlying.IsEnum)
            return true;

        return scalarTypes.Contains(underlying);
    }

    public static bool IsNullable(this Type type)
        => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);

    public static Type UnwrapNullable(this Type type)
        => type.IsNullable() ? Nullable.GetUnderlyingType(type)! : type;
}
