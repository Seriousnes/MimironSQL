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

        return mappingInfo.ClrType switch
        {
            null => null,
            var t when t == typeof(int) => Int32,
            var t when t == typeof(long) => Int64,
            var t when t == typeof(short) => Int16,
            var t when t == typeof(byte) => Byte,
            var t when t == typeof(sbyte) => SByte,
            var t when t == typeof(uint) => UInt32,
            var t when t == typeof(ulong) => UInt64,
            var t when t == typeof(ushort) => UInt16,
            var t when t == typeof(bool) => Boolean,
            var t when t == typeof(string) => String,
            var t when t == typeof(float) => Single,
            var t when t == typeof(double) => Double,
            var t when t == typeof(decimal) => Decimal,
            var t when t == typeof(DateTime) => DateTime,
            var t when t == typeof(Guid) => Guid,
            _ => null,
        };
    }
}
