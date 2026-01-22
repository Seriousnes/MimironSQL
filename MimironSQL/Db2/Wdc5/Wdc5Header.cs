using MimironSQL.Db2;

namespace MimironSQL.Db2.Wdc5;

public readonly record struct Wdc5Header(
    uint SchemaVersion,
    string SchemaString,
    int RecordsCount,
    int FieldsCount,
    int RecordSize,
    int StringTableSize,
    uint TableHash,
    uint LayoutHash,
    int MinIndex,
    int MaxIndex,
    int Locale,
    Db2Flags Flags,
    ushort IdFieldIndex,
    int TotalFieldsCount,
    int PackedDataOffset,
    int LookupColumnCount,
    int ColumnMetaDataSize,
    int CommonDataSize,
    int PalletDataSize,
    int SectionsCount);
