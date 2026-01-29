using System.Reflection;

using MimironSQL.Db2.Query;

namespace MimironSQL.Extensions;

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

    private static readonly IReadOnlyDictionary<Type, MethodInfo> readMethods = new Dictionary<Type, MethodInfo>
    {
        [typeof(string)] = typeof(Db2RowValue).GetMethod(nameof(Db2RowValue.ReadString), BindingFlags.Public | BindingFlags.Static)!,
        [typeof(float)] = typeof(Db2RowValue).GetMethod(nameof(Db2RowValue.ReadSingle), BindingFlags.Public | BindingFlags.Static)!,
        [typeof(double)] = typeof(Db2RowValue).GetMethod(nameof(Db2RowValue.ReadDouble), BindingFlags.Public | BindingFlags.Static)!,
        [typeof(bool)] = typeof(Db2RowValue).GetMethod(nameof(Db2RowValue.ReadBoolean), BindingFlags.Public | BindingFlags.Static)!,
        [typeof(byte)] = typeof(Db2RowValue).GetMethod(nameof(Db2RowValue.ReadByte), BindingFlags.Public | BindingFlags.Static)!,
        [typeof(sbyte)] = typeof(Db2RowValue).GetMethod(nameof(Db2RowValue.ReadSByte), BindingFlags.Public | BindingFlags.Static)!,
        [typeof(short)] = typeof(Db2RowValue).GetMethod(nameof(Db2RowValue.ReadInt16), BindingFlags.Public | BindingFlags.Static)!,
        [typeof(ushort)] = typeof(Db2RowValue).GetMethod(nameof(Db2RowValue.ReadUInt16), BindingFlags.Public | BindingFlags.Static)!,
        [typeof(int)] = typeof(Db2RowValue).GetMethod(nameof(Db2RowValue.ReadInt32), BindingFlags.Public | BindingFlags.Static)!,
        [typeof(uint)] = typeof(Db2RowValue).GetMethod(nameof(Db2RowValue.ReadUInt32), BindingFlags.Public | BindingFlags.Static)!,
        [typeof(long)] = typeof(Db2RowValue).GetMethod(nameof(Db2RowValue.ReadInt64), BindingFlags.Public | BindingFlags.Static)!,
        [typeof(ulong)] = typeof(Db2RowValue).GetMethod(nameof(Db2RowValue.ReadUInt64), BindingFlags.Public | BindingFlags.Static)!,
    };

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

    public static MethodInfo GetReadMethod(this Type readType)
    {
        var underlying = readType.UnwrapNullable();
        if (underlying.IsEnum)
            underlying = Enum.GetUnderlyingType(underlying);

        if (readMethods.TryGetValue(underlying, out var method))
            return method;

        throw new NotSupportedException($"Unsupported read type {readType.FullName}.");
    }
}
