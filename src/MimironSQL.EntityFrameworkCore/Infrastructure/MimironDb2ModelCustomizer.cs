using System.ComponentModel.DataAnnotations.Schema;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Db2;

namespace MimironSQL.EntityFrameworkCore.Infrastructure;

/// <summary>
/// Applies provider-specific model conventions for MimironDB2.
/// </summary>
/// <param name="dependencies">EF Core model customizer dependencies.</param>
public class MimironDb2ModelCustomizer(ModelCustomizerDependencies dependencies) : ModelCustomizer(dependencies)
{
    /// <inheritdoc />
    public override void Customize(ModelBuilder modelBuilder, DbContext context)
    {
        base.Customize(modelBuilder, context);

        foreach (var entityType in modelBuilder.Model.GetEntityTypes())
        {
            ApplyDb2TableConventions(entityType);
        }

        PrecompileMaterializers(modelBuilder, context);
    }

    private static void PrecompileMaterializers(ModelBuilder modelBuilder, DbContext context)
    {
        var store = context.GetService<IMimironDb2Store>();

        var binding = new Db2ModelBinding(
            (Microsoft.EntityFrameworkCore.Metadata.IModel)modelBuilder.Model,
            tableName => store.GetSchema(tableName));

        foreach (var efEntityType in modelBuilder.Model.GetEntityTypes())
        {
            var clrType = efEntityType.ClrType;

            if (!IsDb2EntityInt(clrType))
                continue;

            var tableName = efEntityType.GetTableName() ?? clrType.Name;

            try
            {
                var schema = store.GetSchema(tableName);
                var db2EntityType = binding.GetEntityType(clrType).WithSchema(tableName, schema);
                Db2EntityMaterializerCache.Precompile((Microsoft.EntityFrameworkCore.Metadata.IModel)modelBuilder.Model, db2EntityType);
            }
            catch
            {
                // Only precompile materializers for entity types that successfully resolve schema.
            }
        }
    }

    private static bool IsDb2EntityInt(Type clrType)
    {
        var current = clrType;
        while (current is not null)
        {
            if (current.IsGenericType && current.GetGenericTypeDefinition() == typeof(Db2Entity<>))
                return current.GetGenericArguments()[0] == typeof(int);

            current = current.BaseType;
        }

        return false;
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

        foreach (var property in entityType.GetProperties())
        {
            if (property.PropertyInfo is null)
                continue;

            var columnAttr = property.PropertyInfo.GetCustomAttributes(typeof(ColumnAttribute), inherit: true)
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
