using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace MimironSQL.EntityFrameworkCore.Storage.Internal;

internal sealed class MimironDb2TypeMapping : CoreTypeMapping
{
    public MimironDb2TypeMapping(
        Type clrType,
        ValueComparer? comparer = null,
        ValueComparer? keyComparer = null,
        JsonValueReaderWriter? jsonValueReaderWriter = null)
        : base(new CoreTypeMappingParameters(
            clrType,
            converter: null,
            comparer,
            keyComparer,
            jsonValueReaderWriter: jsonValueReaderWriter))
    {
    }

    private MimironDb2TypeMapping(CoreTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    public override CoreTypeMapping WithComposedConverter(
        ValueConverter? converter,
        ValueComparer? comparer = null,
        ValueComparer? keyComparer = null,
        CoreTypeMapping? elementMapping = null,
        JsonValueReaderWriter? jsonValueReaderWriter = null)
        => new MimironDb2TypeMapping(
            Parameters.WithComposedConverter(converter, comparer, keyComparer, elementMapping, jsonValueReaderWriter));

    protected override CoreTypeMapping Clone(CoreTypeMappingParameters parameters)
        => new MimironDb2TypeMapping(parameters);
}
