using System.ComponentModel.DataAnnotations.Schema;

using MimironSQL.Db2;
using MimironSQL.Db2.Model;
using MimironSQL.Db2.Query;
using MimironSQL.Formats;
using MimironSQL.Providers;

using Shouldly;

namespace MimironSQL.Tests;

public sealed class ColumnAndTableMappingTests
{
    [Fact]
    public void ColumnAttribute_maps_property_to_dbd_field_case_insensitive()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var context = new ColumnAttributeSpellNameContext(dbdProvider, db2Provider);

        var entityType = context.Model.GetEntityType(typeof(SpellName_ColumnAttribute));
        entityType.TableName.ShouldBe("SpellName");

        var (file, schema) = context.GetOrOpenTableRawTyped<RowHandle>(entityType.TableName);
        schema.TryGetFieldCaseInsensitive("Name_lang", out var nameField).ShouldBeTrue();

        var (candidateId, expectedName) = FindFirstNonEmptyString(file, nameField.ColumnStartIndex);

        var actual = context.SpellNames
            .Where(x => x.Id == candidateId)
            .Select(x => x.Name)
            .Single();

        actual.ShouldBe(expectedName);
    }

    [Fact]
    public void FluentPropertyMapping_maps_property_to_dbd_field()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var context = new FluentColumnSpellNameContext(dbdProvider, db2Provider);

        var entityType = context.Model.GetEntityType(typeof(SpellName_Fluent));
        entityType.TableName.ShouldBe("SpellName");

        var (file, schema) = context.GetOrOpenTableRawTyped<RowHandle>(entityType.TableName);
        schema.TryGetFieldCaseInsensitive("Name_lang", out var nameField).ShouldBeTrue();

        var (candidateId, expectedName) = FindFirstNonEmptyString(file, nameField.ColumnStartIndex);

        var actual = context.SpellNames
            .Where(x => x.Id == candidateId)
            .Select(x => x.Name)
            .Single();

        actual.ShouldBe(expectedName);
    }

    [Fact]
    public void ColumnAttribute_conflicts_with_fluent_column_mapping()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var ex = Should.Throw<NotSupportedException>(() => new ColumnAttributeAndFluentConflictContext(dbdProvider, db2Provider));
        ex.Message.ShouldContain("[Column]");
        ex.Message.ShouldContain(nameof(SpellName_ColumnAttributeAndFluentConflict.Name));
    }

    [Fact]
    public void TableAttribute_conflicts_with_ToTable()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var ex = Should.Throw<NotSupportedException>(() => new TableAttributeAndToTableConflictContext(dbdProvider, db2Provider));
        ex.Message.ShouldContain("[Table]");
        ex.Message.ShouldContain("ToTable");
    }

    [Fact]
    public void ColumnAttribute_is_not_supported_on_fields()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var ex = Should.Throw<NotSupportedException>(() => new InvalidColumnAttributeTargetContext(dbdProvider, db2Provider));
        ex.Message.ShouldContain("Field");
        ex.Message.ShouldContain("Column mapping attributes");
    }

    [Fact]
    public void Fluent_column_mapping_is_not_supported_on_primary_key()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var ex = Should.Throw<NotSupportedException>(() => new PrimaryKeyFluentColumnMappingContext(dbdProvider, db2Provider));
        ex.Message.ShouldContain("primary key", Case.Insensitive);
    }

    private static (int Id, string Value) FindFirstNonEmptyString(IDb2File<RowHandle> file, int fieldIndex)
    {
        foreach (var row in file.EnumerateRows())
        {
            var handle = Db2RowHandleAccess.AsHandle(row);
            if (handle.RowId == 0)
                continue;

            var value = Db2RowHandleAccess.ReadField<RowHandle, string>(file, row, fieldIndex);
            if (!string.IsNullOrWhiteSpace(value))
                return (handle.RowId, value);
        }

        throw new InvalidOperationException("No non-empty string row found in SpellName fixture.");
    }
}

[Table("SpellName")]
internal sealed class SpellName_ColumnAttribute : Db2Entity
{
    [Column("name_LANG")]
    public string Name { get; set; } = string.Empty;
}

internal sealed class ColumnAttributeSpellNameContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider)
{
    public Db2Table<SpellName_ColumnAttribute> SpellNames { get; init; } = null!;
}

internal sealed class SpellName_Fluent : Db2Entity
{
    public string Name { get; set; } = string.Empty;
}

internal sealed class FluentColumnSpellNameContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider)
{
    public Db2Table<SpellName_Fluent> SpellNames { get; init; } = null!;

    protected override void OnModelCreating(Db2ModelBuilder modelBuilder)
        => modelBuilder
            .Entity<SpellName_Fluent>()
            .ToTable("SpellName")
            .Property(x => x.Name)
            .HasColumnName("Name_lang");
}

[Table("SpellName")]
internal sealed class SpellName_ColumnAttributeAndFluentConflict : Db2Entity
{
    [Column("Name_lang")]
    public string Name { get; set; } = string.Empty;
}

internal sealed class ColumnAttributeAndFluentConflictContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider)
{
    public Db2Table<SpellName_ColumnAttributeAndFluentConflict> SpellNames { get; init; } = null!;

    protected override void OnModelCreating(Db2ModelBuilder modelBuilder)
        => modelBuilder
            .Entity<SpellName_ColumnAttributeAndFluentConflict>()
            .Property(x => x.Name)
            .HasColumnName("Name_lang");
}

[Table("SpellName")]
internal sealed class SpellName_TableAttributeAndToTableConflict : Db2Entity
{
    public string Name_lang { get; set; } = string.Empty;
}

internal sealed class TableAttributeAndToTableConflictContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider)
{
    public Db2Table<SpellName_TableAttributeAndToTableConflict> SpellNames { get; init; } = null!;

    protected override void OnModelCreating(Db2ModelBuilder modelBuilder)
        => modelBuilder.Entity<SpellName_TableAttributeAndToTableConflict>().ToTable("SpellName");
}

[Table("SpellName")]
internal sealed class SpellName_InvalidFieldColumnAttributeTarget : Db2Entity
{
    [Column("Name_lang")]
    public readonly string Name_lang = string.Empty;
}

internal sealed class InvalidColumnAttributeTargetContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider)
{
    public Db2Table<SpellName_InvalidFieldColumnAttributeTarget> SpellNames { get; init; } = null!;
}

internal sealed class SpellName_PrimaryKeyFluentColumnMapping : Db2Entity
{
    public string Name_lang { get; set; } = string.Empty;
}

internal sealed class PrimaryKeyFluentColumnMappingContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider)
{
    public Db2Table<SpellName_PrimaryKeyFluentColumnMapping> SpellNames { get; init; } = null!;

    protected override void OnModelCreating(Db2ModelBuilder modelBuilder)
        => modelBuilder
            .Entity<SpellName_PrimaryKeyFluentColumnMapping>()
            .ToTable("SpellName")
            .Property(x => x.Id)
            .HasColumnName("ID");
}
