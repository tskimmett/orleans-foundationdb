using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Streaming.FoundationDb;

[SerializationCallbacks(typeof(OnDeserializedCallbacks))]
public class FdbQueueDataAdapter(Serializer serializer) : IOnDeserialized
{
	Serializer<FdbQueueBatchContainer> serializer = serializer.GetSerializer<FdbQueueBatchContainer>();

	public Slice ToQueueMessage<T>(
		StreamId streamId,
		IEnumerable<T> events,
		StreamSequenceToken token,	// unused
		Dictionary<string, object> requestContext)
	{
		var batchMessage = new FdbQueueBatchContainer(streamId, events.Cast<object>().ToList(), requestContext);
		return Slice.Copy(serializer.SerializeToArray(batchMessage));
	}

	public IBatchContainer FromQueueMessage(VersionStamp stamp, Slice data)
	{
		var queueBatch = serializer.Deserialize(data);
		queueBatch.RealSequenceToken = new FdbStreamSequenceToken(stamp);
		return queueBatch;
	}

	void IOnDeserialized.OnDeserialized(DeserializationContext context)
	{
		serializer = context.ServiceProvider.GetRequiredService<Serializer<FdbQueueBatchContainer>>();
	}
}