using System.Reflection;

namespace MimironSQL.Extensions;

internal static class MemberInfoExtensions
{
    public static Type GetMemberType(this MemberInfo member)
        => member switch
        {
            PropertyInfo p => p.PropertyType,
            FieldInfo f => f.FieldType,
            _ => throw new InvalidOperationException($"Unexpected member type: {member.GetType().FullName}"),
        };
}
