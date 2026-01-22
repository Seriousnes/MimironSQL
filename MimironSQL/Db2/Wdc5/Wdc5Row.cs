using MimironSQL.Db2;
using System;
using System.Collections.Generic;

namespace MimironSQL.Db2.Wdc5;

public readonly struct Wdc5Row
{
    private readonly Wdc5File _file;
    private readonly Wdc5Section _section;
    private readonly BitReader _reader;

    public int GlobalRowIndex { get; }
    public int RowIndexInSection { get; }
    public int Id { get; }

    internal Wdc5Row(Wdc5File file, Wdc5Section section, BitReader reader, int globalRowIndex, int rowIndexInSection, int id)
    {
        _file = file;
        _section = section;
        _reader = reader;
        GlobalRowIndex = globalRowIndex;
        RowIndexInSection = rowIndexInSection;
        Id = id;
    }

    public T GetScalar<T>(int fieldIndex) where T : unmanaged
    {
        if ((uint)fieldIndex >= (uint)_file.Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        var localReader = _reader;

        // Sequential decode to match the file layout expectations.
        for (var i = 0; i <= fieldIndex; i++)
        {
            ref readonly var fieldMeta = ref _file.FieldMeta[i];
            ref readonly var columnMeta = ref _file.ColumnMeta[i];
            var palletData = _file.PalletData[i];
            var commonData = _file.CommonData[i];

            if (i == fieldIndex)
                return Wdc5FieldDecoder.ReadScalar<T>(Id, ref localReader, fieldMeta, columnMeta, palletData, commonData);

            _ = Wdc5FieldDecoder.ReadScalar<uint>(Id, ref localReader, fieldMeta, columnMeta, palletData, commonData);
        }

        throw new InvalidOperationException("Unreachable.");
    }
}
