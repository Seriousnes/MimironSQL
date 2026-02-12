using Microsoft.EntityFrameworkCore.Storage;

namespace MimironSQL.EntityFrameworkCore.Storage;

internal sealed class MimironDb2TypeMappingSource(TypeMappingSourceDependencies dependencies) : TypeMappingSource(dependencies)
{
    private static readonly CoreTypeMapping Int32 = new MimironDb2TypeMapping(typeof(int));
    private static readonly CoreTypeMapping Int64 = new MimironDb2TypeMapping(typeof(long));
    private static readonly CoreTypeMapping Int16 = new MimironDb2TypeMapping(typeof(short));
    private static readonly CoreTypeMapping Byte = new MimironDb2TypeMapping(typeof(byte));
    private static readonly CoreTypeMapping SByte = new MimironDb2TypeMapping(typeof(sbyte));
    private static readonly CoreTypeMapping UInt32 = new MimironDb2TypeMapping(typeof(uint));
    private static readonly CoreTypeMapping UInt64 = new MimironDb2TypeMapping(typeof(ulong));
    private static readonly CoreTypeMapping UInt16 = new MimironDb2TypeMapping(typeof(ushort));
    private static readonly CoreTypeMapping Boolean = new MimironDb2TypeMapping(typeof(bool));
    private static readonly CoreTypeMapping String = new MimironDb2TypeMapping(typeof(string));
    private static readonly CoreTypeMapping Single = new MimironDb2TypeMapping(typeof(float));
    private static readonly CoreTypeMapping Double = new MimironDb2TypeMapping(typeof(double));
    private static readonly CoreTypeMapping Decimal = new MimironDb2TypeMapping(typeof(decimal));
    private static readonly CoreTypeMapping DateTime = new MimironDb2TypeMapping(typeof(DateTime));
    private static readonly CoreTypeMapping Guid = new MimironDb2TypeMapping(typeof(Guid));

    protected override CoreTypeMapping? FindMapping(in TypeMappingInfo mappingInfo)
    {
        var mapping = base.FindMapping(mappingInfo);
        if (mapping is not null)
            return mapping;

        var clrType = mappingInfo.ClrType;
        if (clrType is null)
            return null;

        if (TryGetScalarMapping(clrType, out var scalar))
            return scalar;

        // Support array-like DB2 schema fields (ElementCount > 1).
        if (clrType.IsArray)
        {
            var elementType = clrType.GetElementType();
            if (elementType is not null && TryGetPrimitiveElementMapping(elementType, out _))
                return new MimironDb2TypeMapping(clrType);
        }

        if (clrType.IsGenericType && clrType.GetGenericTypeDefinition() == typeof(ICollection<>))
        {
            var elementType = clrType.GetGenericArguments()[0];
            if (TryGetPrimitiveElementMapping(elementType, out _))
                return new MimironDb2TypeMapping(clrType);
        }

        return null;
    }

    private static bool TryGetScalarMapping(Type clrType, out CoreTypeMapping mapping)
    {
        if (clrType == typeof(int))
        {
            mapping = Int32;
            return true;
        }

        if (clrType == typeof(long))
        {
            mapping = Int64;
            return true;
        }

        if (clrType == typeof(short))
        {
            mapping = Int16;
            return true;
        }

        if (clrType == typeof(byte))
        {
            mapping = Byte;
            return true;
        }

        if (clrType == typeof(sbyte))
        {
            mapping = SByte;
            return true;
        }

        if (clrType == typeof(uint))
        {
            mapping = UInt32;
            return true;
        }

        if (clrType == typeof(ulong))
        {
            mapping = UInt64;
            return true;
        }

        if (clrType == typeof(ushort))
        {
            mapping = UInt16;
            return true;
        }

        if (clrType == typeof(bool))
        {
            mapping = Boolean;
            return true;
        }

        if (clrType == typeof(string))
        {
            mapping = String;
            return true;
        }

        if (clrType == typeof(float))
        {
            mapping = Single;
            return true;
        }

        if (clrType == typeof(double))
        {
            mapping = Double;
            return true;
        }

        if (clrType == typeof(decimal))
        {
            mapping = Decimal;
            return true;
        }

        if (clrType == typeof(DateTime))
        {
            mapping = DateTime;
            return true;
        }

        if (clrType == typeof(Guid))
        {
            mapping = Guid;
            return true;
        }

        mapping = null!;
        return false;
    }

    private static bool TryGetPrimitiveElementMapping(Type elementType, out CoreTypeMapping mapping)
    {
        if (elementType == typeof(string))
        {
            mapping = null!;
            return false;
        }

        return TryGetScalarMapping(elementType, out mapping);
    }
}
