using System.Collections.Concurrent;
using System.Globalization;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streaming.FoundationDb;
using Orleans.Streams;
using TestExtensions;
using Xunit.Abstractions;

namespace Orleans.FoundationDb.Tests;

[Collection(TestEnvironmentFixture.DefaultCollection)]
public class QueueAdapterTests : IClassFixture<FdbFixture>
{
	const int NumBatches = 20;
	const int NumMessagesPerBatch = 20;
	public const string FdbStreamProviderName = "FdbQueueAdapterTests";

	readonly ITestOutputHelper output;
	readonly TestEnvironmentFixture fixture;
	readonly FdbFixture fdbFixture;

	readonly ILoggerFactory loggerFactory;

	public QueueAdapterTests(ITestOutputHelper output, TestEnvironmentFixture fixture, FdbFixture fdbFixture)
	{
		this.output = output;
		this.fixture = fixture;
		this.fdbFixture = fdbFixture;
		loggerFactory = this.fixture.Services.GetRequiredService<ILoggerFactory>();
	}

	[SkippableFact]
	public async Task SendAndReceiveFromFdbQueue()
	{
		var serializer = fixture.Services.GetRequiredService<Serializer>();
		var queueCacheOptions = new SimpleQueueCacheOptions();
		var queueMapperOptions = new HashRingStreamQueueMapperOptions();
		var queueDataAdapter = new FdbQueueDataAdapter(serializer);
		var adapterFactory = new FdbQueueAdapterFactory(
			FdbStreamProviderName,
			fdbFixture.Provider,
			queueDataAdapter,
			new FdbStreamFailureHandler(loggerFactory.CreateLogger<FdbStreamFailureHandler>()),
			queueCacheOptions,
			queueMapperOptions,
			loggerFactory);
		await SendAndReceiveFromQueueAdapter(adapterFactory);
	}

	async Task SendAndReceiveFromQueueAdapter(IQueueAdapterFactory adapterFactory)
	{
		IQueueAdapter adapter = await adapterFactory.CreateAdapter();
		IQueueAdapterCache cache = adapterFactory.GetQueueAdapterCache();

		// Create receiver per queue
		var mapper = adapterFactory.GetStreamQueueMapper();
		var receivers = mapper.GetAllQueues().ToDictionary(queueId => queueId, adapter.CreateReceiver);
		var caches = mapper.GetAllQueues().ToDictionary(queueId => queueId, cache.CreateQueueCache);
		var streamStarts = new ConcurrentDictionary<QueueId, StreamSequenceToken>();

		await Task.WhenAll(receivers.Values.Select(receiver => receiver.Initialize(TimeSpan.FromSeconds(5))));

		// test using 2 streams
		Guid streamId1 = Guid.NewGuid();
		Guid streamId2 = Guid.NewGuid();

		int receivedBatches = 0;
		var streamsPerQueue = new ConcurrentDictionary<QueueId, HashSet<StreamId>>();

		// reader threads (at most 2 active queues because only two streams)
		var work = new List<Task>();
		foreach (KeyValuePair<QueueId, IQueueAdapterReceiver> receiverKvp in receivers)
		{
			QueueId queueId = receiverKvp.Key;
			var receiver = receiverKvp.Value;
			var qCache = caches[queueId];
			Task task = Task.Factory.StartNew(() =>
			{
				while (receivedBatches < NumBatches)
				{
					var messages = receiver.GetQueueMessagesAsync(QueueAdapterConstants.UNLIMITED_GET_QUEUE_MSG).Result.ToArray();
					if (!messages.Any())
					{
						continue;
					}

					foreach (IBatchContainer message in messages)
					{
						streamStarts.TryAdd(queueId, message.SequenceToken);
						
						streamsPerQueue.AddOrUpdate(queueId,
							id => new HashSet<StreamId> { message.StreamId },
							(id, set) =>
							{
								set.Add(message.StreamId);
								return set;
							});
						output.WriteLine("Queue {0} received message on stream {1}", queueId,
							message.StreamId);
						Assert.Equal(NumMessagesPerBatch / 2,
							message.GetEvents<int>().Count()); // "Half the events were ints"
						Assert.Equal(NumMessagesPerBatch / 2,
							message.GetEvents<string>().Count()); // "Half the events were strings"
					}

					Interlocked.Add(ref receivedBatches, messages.Length);
					qCache.AddToCache(messages);
				}
			});
			work.Add(task);
		}

		// send events
		List<object> events = CreateEvents(NumMessagesPerBatch);
		work.Add(Task.Factory.StartNew(() => Enumerable.Range(0, NumBatches)
			.Select(i => i % 2 == 0 ? streamId1 : streamId2)
			.ToList()
			.ForEach(streamId =>
				adapter.QueueMessageBatchAsync(StreamId.Create(streamId.ToString(), streamId),
					events.Take(NumMessagesPerBatch).ToArray(), null,
					RequestContextExtensions.Export(fixture.DeepCopier)).Wait())));
		await Task.WhenAll(work);

		// Make sure we got back everything we sent
		Assert.Equal(NumBatches, receivedBatches);

		// check to see if all the events are in the cache and we can enumerate through them
		foreach (KeyValuePair<QueueId, HashSet<StreamId>> kvp in streamsPerQueue)
		{
			var receiver = receivers[kvp.Key];
			var qCache = caches[kvp.Key];
			StreamSequenceToken firstInCache = streamStarts[kvp.Key];

			foreach (StreamId streamGuid in kvp.Value)
			{
				// read all messages in cache for stream
				IQueueCacheCursor cursor = qCache.GetCacheCursor(streamGuid, firstInCache);
				int messageCount = 0;
				StreamSequenceToken? tenthInCache = null;
				StreamSequenceToken lastToken = firstInCache;
				while (cursor.MoveNext())
				{
					Exception ex;
					messageCount++;
					IBatchContainer batch = cursor.GetCurrent(out ex);
					output.WriteLine("Token: {0}", batch.SequenceToken);
					Assert.True(batch.SequenceToken.CompareTo(lastToken) >= 0, $"order check for event {messageCount}");
					lastToken = batch.SequenceToken;
					if (messageCount == 10)
					{
						tenthInCache = batch.SequenceToken;
					}
				}

				output.WriteLine("On Queue {0} we received a total of {1} message on stream {2}", kvp.Key,
					messageCount, streamGuid);
				Assert.Equal(NumBatches / 2, messageCount);
				Assert.NotNull(tenthInCache);

				// read all messages from the 10th
				cursor = qCache.GetCacheCursor(streamGuid, tenthInCache);
				messageCount = 0;
				while (cursor.MoveNext())
				{
					messageCount++;
				}

				output.WriteLine("On Queue {0} we received a total of {1} message on stream {2} after 10th", kvp.Key,
					messageCount, streamGuid);
				const int expected = NumBatches / 2 - 10 + 1; // all except the first 10, including the 10th (10 + 1)
				Assert.Equal(expected, messageCount);
			}
		}
	}

	List<object> CreateEvents(int count)
	{
		return Enumerable.Range(0, count).Select(i =>
		{
			if (i % 2 == 0)
			{
				return Random.Shared.Next(int.MaxValue) as object;
			}

			return Random.Shared.Next(int.MaxValue).ToString(CultureInfo.InvariantCulture);
		}).ToList();
	}

	internal static string MakeClusterId()
	{
		const string deploymentIdFormat = "cluster-{0}";
		string now = DateTime.UtcNow.ToString("yyyy-MM-dd-hh-mm-ss-ffff");
		return string.Format(deploymentIdFormat, now);
	}
}