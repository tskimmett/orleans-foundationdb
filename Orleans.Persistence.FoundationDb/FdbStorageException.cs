namespace Orleans.Persistence.FoundationDb;

[Serializable]
internal class FdbStorageException : Exception
{
    private object value;

    public FdbStorageException()
    {
    }

    public FdbStorageException(object value)
    {
        this.value = value;
    }

    public FdbStorageException(string? message) : base(message)
    {
    }

    public FdbStorageException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}