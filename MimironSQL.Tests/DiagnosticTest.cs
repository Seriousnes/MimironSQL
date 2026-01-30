using MimironSQL.Db2;
using MimironSQL.Db2.Schema;
using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;
using MimironSQL.Providers;

using Xunit.Abstractions;

namespace MimironSQL.Tests;

public sealed class DiagnosticTest(ITestOutputHelper output)
{
    [Fact]
    public void Diagnose_dense_string_reading()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var stream = db2Provider.OpenDb2Stream("Map");
        var file = new Wdc5File(stream);

        output.WriteLine($"Header.StringTableSize = {file.Header.StringTableSize}");
        output.WriteLine($"DenseStringTableBytes.Length = {file.DenseStringTableBytes.Length}");
        output.WriteLine($"RecordsCount = {file.Header.RecordsCount}");
        output.WriteLine($"Sparse = {file.Header.Flags.HasFlag(Db2Flags.Sparse)}");

        var provider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(TestDataPaths.GetTestDataDirectory()));
        var mapper = new SchemaMapper(provider);
        var schema = mapper.GetSchema("Map", new Db2FileLayout(file.Header.LayoutHash, file.Header.FieldsCount));
        schema.TryGetField("Directory", out var directory);

        output.WriteLine($"Directory field: ColumnStartIndex={directory.ColumnStartIndex}, IsVirtual={directory.IsVirtual}");

        int tryCount = 0;
        int successCount = 0;
        foreach (var row in file.EnumerateRows().Take(10))
        {
            tryCount++;
            var id = row.RowId;

            string? value = null;
            var success = false;
            try
            {
                value = file.ReadField<string>(row, directory.ColumnStartIndex);
                success = !string.IsNullOrEmpty(value);
            }
            catch
            {
            }

            if (success)
                successCount++;

            output.WriteLine($"Row {id}: Get<string> success={success}, value='{value}'");
        }
        output.WriteLine($"Tried {tryCount}, succeeded {successCount}");
    }
}
