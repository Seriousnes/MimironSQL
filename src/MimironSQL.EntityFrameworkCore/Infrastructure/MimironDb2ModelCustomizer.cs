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

        var tableAttr = clrType.GetCustomAttributes(typeof(TableAttribute), inherit: true)
            .OfType<TableAttribute>()
            .FirstOrDefault();

        var currentTableName = entityType.GetTableName();

        if (tableAttr is not null && !string.IsNullOrWhiteSpace(tableAttr.Name))
        {
            // EF may have already assigned the default table name (CLR type name). Ensure
            // [Table] is still applied for this provider.
            if (!string.Equals(currentTableName, tableAttr.Name, StringComparison.Ordinal))
                entityType.SetTableName(tableAttr.Name);
        }
        else if (currentTableName is null)
        {
            entityType.SetTableName(clrType.Name);
        }

        foreach (var property in entityType.GetProperties().Where(static p => p.PropertyInfo is not null))
        {
            var columnAttr = property.PropertyInfo!.GetCustomAttributes(typeof(ColumnAttribute), inherit: true)
                .OfType<ColumnAttribute>()
                .FirstOrDefault();

            var currentColumnName = property.GetColumnName();

            if (columnAttr is not null && !string.IsNullOrWhiteSpace(columnAttr.Name))
            {
                if (!string.Equals(currentColumnName, columnAttr.Name, StringComparison.Ordinal))
                    property.SetColumnName(columnAttr.Name);
                continue;
            }

            if (currentColumnName is null)
            {
                property.SetColumnName(property.PropertyInfo.Name);
            }
        }
    }
}
