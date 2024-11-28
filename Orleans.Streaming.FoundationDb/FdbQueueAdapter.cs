using FoundationDB.Client;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Streaming.FoundationDb;

public class FdbQueueAdapter(
	string providerName,
	IFdbDatabaseProvider fdb,
	FdbQueueDataAdapter dataAdapter,
	HashRingBasedStreamQueueMapper queueMapper,
	ILogger<FdbQueueAdapter> logger,
	ILoggerFactory loggerFactory
) : IQueueAdapter
{
	bool initialized = false;
	public const string DirName = "orleans-streaming";

	public string Name { get; } = providerName;
	public bool IsRewindable => false;
	public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

	public async Task QueueMessageBatchAsync<T>(
		StreamId streamId,
		IEnumerable<T> events,
		StreamSequenceToken token, // unused
		Dictionary<string, object> requestContext)
	{
		var queueId = queueMapper.GetQueueForStream(streamId).ToString();
		var message = dataAdapter.ToQueueMessage(streamId, events, null!, requestContext);
		await fdb.WriteAsync(async tx =>
		{
			var streamingDir = fdb.Root[DirName];
			var dir = initialized
				? await streamingDir.Resolve(tx)
				: await streamingDir.CreateOrOpenAsync(tx);
			tx.SetVersionStampedKey(dir!.Encode(queueId, tx.CreateVersionStamp().ToSlice()), message);
		}, new());
		initialized = true;
	}

	public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
	{
		return new FdbQueueReceiver(queueId, fdb, dataAdapter, loggerFactory.CreateLogger<FdbQueueReceiver>());
	}
}