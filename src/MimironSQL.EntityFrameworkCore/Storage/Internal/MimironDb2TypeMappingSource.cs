using Microsoft.EntityFrameworkCore.Storage;

namespace MimironSQL.EntityFrameworkCore.Storage.Internal;

internal sealed class MimironDb2TypeMappingSource(TypeMappingSourceDependencies dependencies) : TypeMappingSource(dependencies)
{
    protected override CoreTypeMapping? FindMapping(in TypeMappingInfo mappingInfo)
    {
        var clrType = mappingInfo.ClrType;
        if (clrType is null)
            return null;

        var jsonValueReaderWriter = Dependencies.JsonValueReaderWriterSource.FindReaderWriter(clrType);

        if (clrType.IsValueType
            || clrType == typeof(string)
            || (clrType == typeof(byte[]) && mappingInfo.ElementTypeMapping is null))
        {
            return new MimironDb2TypeMapping(clrType, jsonValueReaderWriter: jsonValueReaderWriter);
        }

        return base.FindMapping(mappingInfo);
    }
}
