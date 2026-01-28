namespace MimironSQL.Formats;

public interface IDb2Row
{
    T Get<T>(int columnIndex);
    
    T[] GetArray<T>(int columnIndex, int elementCount) where T : unmanaged;
    
    string GetString(int columnIndex);
}
