using FoundationDB.Client;
using Microsoft.Extensions.Logging;
using Orleans.Streams;

namespace Orleans.Streaming.FoundationDb;

public class FdbQueueReceiver(
	QueueId queueId,
	IFdbDatabaseProvider fdb,
	FdbQueueDataAdapter dataAdapter,
	ILogger<FdbQueueReceiver> logger
) : IQueueAdapterReceiver
{
	const int MaxDequeueBytes = 1_000_000;

	bool isShutdown = false;
	readonly string queueKey = queueId.ToString();
	VersionStamp? lastRead;
	Task outstandingTask = Task.CompletedTask;

	public async Task Initialize(TimeSpan timeout)
	{
		try
		{
			using var cts = new CancellationTokenSource(timeout);
			await fdb.WriteAsync(tx => fdb.Root[FdbQueueAdapter.DirName].CreateOrOpenAsync(tx), cts.Token);
		}
		catch (Exception ex)
		{
			logger.LogError(ex, "Error initializing stream {QueueId}", queueId);
		}
	}

	public async Task<IList<IBatchContainer>?> GetQueueMessagesAsync(int maxCount)
	{
		if (isShutdown)
			throw new Exception(
				$"{nameof(FdbQueueReceiver)} can no longer get messages for queue {queueId} since it has been shutdown.");

		try
		{
			var results = fdb.ReadAsync(async tx =>
			{
				if (isShutdown)
					return null;
				
				var dir = await fdb.Root[FdbQueueAdapter.DirName].Resolve(tx);
				var rangeOptions = new FdbRangeOptions { Limit = maxCount > 0 ? maxCount : null, TargetBytes = MaxDequeueBytes };
				var rangeStart = lastRead.HasValue
					? dir!.Encode(queueKey, lastRead) + 1
					: dir!.Encode(queueKey);
				var range = await tx.GetRange(
					rangeStart,
					dir!.EncodeRange(queueKey).End,
					rangeOptions
				).ToListAsync();
				return range.Select(x => (stamp: dir!.DecodeLast<VersionStamp>(x.Key), data: x.Value));
			}, new());

			outstandingTask = results;

			var messages = (await results).ToList();
			if (messages.Count != 0)
				lastRead = messages.LastOrDefault().stamp;
			return messages
				.Select(msg => dataAdapter.FromQueueMessage(msg.stamp, msg.data))
				.ToList();
		}
		catch (Exception ex)
		{
			logger.LogError(ex, "Error reading from queue {QueueId}", queueId);
			return null;
		}
		finally
		{
			outstandingTask = Task.CompletedTask;
		}
	}

	public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
	{
		if (isShutdown || messages.Count == 0)
			return;

		var keysToClear = messages
			.OfType<FdbQueueBatchContainer>()
			.Select(batch => batch.RealSequenceToken.ToVersionStamp().ToSlice())
			.ToList();

		try
		{
			outstandingTask = fdb.WriteAsync(tx =>
			{
				foreach (var key in keysToClear)
					tx.Clear(key);
			}, new());

			await outstandingTask;
		}
		catch (Exception exc)
		{
			logger.LogWarning(exc, "Exception upon deleting messages on queue {QueueId}. Ignoring.", queueId);
		}
		finally
		{
			outstandingTask = Task.CompletedTask;
		}
	}

	public async Task Shutdown(TimeSpan timeout)
	{
		isShutdown = true;
		await outstandingTask;
	}
}