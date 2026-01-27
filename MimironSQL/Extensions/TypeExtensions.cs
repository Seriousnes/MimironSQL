namespace MimironSQL.Extensions;

internal static class TypeExtensions
{
    public static bool IsScalarType(this Type type)
        => type.IsPrimitive || type == typeof(decimal) || type.IsEnum;

    public static bool IsNullable(this Type type)
        => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);

    public static Type UnwrapNullable(this Type type)
        => type.IsNullable() ? Nullable.GetUnderlyingType(type)! : type;
}
