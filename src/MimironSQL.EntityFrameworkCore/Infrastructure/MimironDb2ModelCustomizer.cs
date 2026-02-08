using System.ComponentModel.DataAnnotations.Schema;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace MimironSQL.EntityFrameworkCore.Infrastructure;

public class MimironDb2ModelCustomizer(ModelCustomizerDependencies dependencies) : ModelCustomizer(dependencies)
{
    public override void Customize(ModelBuilder modelBuilder, DbContext context)
    {
        base.Customize(modelBuilder, context);

        foreach (var entityType in modelBuilder.Model.GetEntityTypes())
        {
            ApplyDb2TableConventions(entityType);
        }
    }

    private static void ApplyDb2TableConventions(Microsoft.EntityFrameworkCore.Metadata.IMutableEntityType entityType)
    {
        var clrType = entityType.ClrType;

        if (entityType.GetTableName() is null)
        {
            var tableAttr = clrType.GetCustomAttributes(typeof(TableAttribute), inherit: true)
                .OfType<TableAttribute>()
                .FirstOrDefault();

            if (tableAttr is not null && !string.IsNullOrWhiteSpace(tableAttr.Name))
            {
                entityType.SetTableName(tableAttr.Name);
            }
            else
            {
                entityType.SetTableName(clrType.Name);
            }
        }

        foreach (var property in entityType.GetProperties())
        {
            if (property.GetColumnName() is null && property.PropertyInfo is not null)
            {
                var columnAttr = property.PropertyInfo.GetCustomAttributes(typeof(ColumnAttribute), inherit: true)
                    .OfType<ColumnAttribute>()
                    .FirstOrDefault();

                if (columnAttr is not null && !string.IsNullOrWhiteSpace(columnAttr.Name))
                {
                    property.SetColumnName(columnAttr.Name);
                }
                else
                {
                    property.SetColumnName(property.PropertyInfo.Name);
                }
            }
        }
    }
}
