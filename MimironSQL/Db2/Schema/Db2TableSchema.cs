using System;
using System.Collections.Generic;

namespace MimironSQL.Db2.Schema;

public sealed class Db2TableSchema(string tableName, uint layoutHash, int physicalColumnCount, IReadOnlyList<Db2FieldSchema> fields)
{
    private readonly Dictionary<string, Db2FieldSchema> _fieldsByName = new(StringComparer.Ordinal);

    public string TableName { get; } = tableName;
    public uint LayoutHash { get; } = layoutHash;
    public int PhysicalColumnCount { get; } = physicalColumnCount;
    public IReadOnlyList<Db2FieldSchema> Fields { get; } = fields;

    public bool TryGetField(string name, out Db2FieldSchema field)
    {
        if (_fieldsByName.Count == 0)
        {
            foreach (var f in Fields)
                _fieldsByName.TryAdd(f.Name, f);
        }

        return _fieldsByName.TryGetValue(name, out field);
    }
}
