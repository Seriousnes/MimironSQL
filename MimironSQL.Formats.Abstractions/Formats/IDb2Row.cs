namespace MimironSQL.Formats;

public interface IDb2Row
{
    int Id { get; }
    
    int GetInt32(int columnIndex);
    uint GetUInt32(int columnIndex);
    long GetInt64(int columnIndex);
    ulong GetUInt64(int columnIndex);
    float GetSingle(int columnIndex);
    byte GetByte(int columnIndex);
    short GetInt16(int columnIndex);
    ushort GetUInt16(int columnIndex);
    
    string GetString(int columnIndex);
    
    T[] GetArray<T>(int columnIndex, int elementCount) where T : unmanaged;
}
