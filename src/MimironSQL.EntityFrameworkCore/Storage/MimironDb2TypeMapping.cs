using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace MimironSQL.EntityFrameworkCore.Storage;

internal sealed class MimironDb2TypeMapping : CoreTypeMapping
{
    public MimironDb2TypeMapping(Type clrType)
        : base(new CoreTypeMappingParameters(clrType))
    {
    }

    private MimironDb2TypeMapping(CoreTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override CoreTypeMapping Clone(CoreTypeMappingParameters parameters)
        => new MimironDb2TypeMapping(parameters);

    public override CoreTypeMapping WithComposedConverter(
        ValueConverter? converter,
        ValueComparer? comparer = null,
        ValueComparer? keyComparer = null,
        CoreTypeMapping? elementMapping = null,
        JsonValueReaderWriter? jsonValueReaderWriter = null)
        => Clone(Parameters.WithComposedConverter(converter, comparer, keyComparer, elementMapping, jsonValueReaderWriter));
}
