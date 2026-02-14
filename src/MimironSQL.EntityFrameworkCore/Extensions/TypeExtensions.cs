namespace MimironSQL.EntityFrameworkCore.Extensions;

internal static class TypeExtensions
{
    public static Type UnwrapNullable(this Type type)
        => Nullable.GetUnderlyingType(type) ?? type;

    public static bool IsScalarType(this Type type)
    {
        type = type.UnwrapNullable();

        if (type.IsEnum)
            return true;

        return type.IsPrimitive
            || type == typeof(string)
            || type == typeof(decimal)
            || type == typeof(DateTime)
            || type == typeof(DateTimeOffset)
            || type == typeof(Guid)
            || type == typeof(TimeSpan);
    }
}
