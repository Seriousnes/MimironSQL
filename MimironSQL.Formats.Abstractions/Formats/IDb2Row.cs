namespace MimironSQL.Formats;

public interface IDb2Row
{
    T Get<T>(int fieldIndex);
    object Get(Type type, int fieldIndex);
}
