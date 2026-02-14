using System.Reflection;

namespace MimironSQL.EntityFrameworkCore.Extensions;

internal static class MemberInfoExtensions
{
    public static Type GetMemberType(this MemberInfo member)
        => member switch
        {
            PropertyInfo p => p.PropertyType,
            FieldInfo f => f.FieldType,
            _ => throw new NotSupportedException($"Unsupported member type '{member.MemberType}' for '{member.DeclaringType?.FullName}.{member.Name}'."),
        };
}
