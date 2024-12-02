namespace Orleans.Persistence.FoundationDb;

[Serializable]
class FdbStorageException(string? message) : Exception(message);