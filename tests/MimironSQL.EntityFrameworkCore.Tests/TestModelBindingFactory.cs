using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

using MimironSQL.EntityFrameworkCore.Model;
using MimironSQL.EntityFrameworkCore.Schema;

namespace MimironSQL.EntityFrameworkCore.Tests;

internal static class TestModelBindingFactory
{
    public static Db2ModelBinding CreateBinding(Action<ModelBuilder> configure, Func<string, Db2TableSchema> schemaResolver)
    {
        ArgumentNullException.ThrowIfNull(configure);
        ArgumentNullException.ThrowIfNull(schemaResolver);

        var modelBuilder = new ModelBuilder(new ConventionSet());
        configure(modelBuilder);

        var model = (IModel)modelBuilder.Model;
        return new Db2ModelBinding(model, schemaResolver);
    }
}
