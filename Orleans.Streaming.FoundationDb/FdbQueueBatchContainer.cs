using Newtonsoft.Json;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Streaming.FoundationDb;

[Serializable]
[GenerateSerializer]
public class FdbQueueBatchContainer : IBatchContainer
{
	[JsonProperty]
	[Id(0)]
	FdbStreamSequenceToken sequenceToken;

	[JsonProperty]
	[Id(1)]
	readonly List<object> events;

	[JsonProperty]
	[Id(2)]
	readonly Dictionary<string, object> requestContext;

	[Id(3)]
	public StreamId StreamId { get; }

	public StreamSequenceToken SequenceToken => sequenceToken;

	// This will only be set on read since the database generates the token
	internal FdbStreamSequenceToken RealSequenceToken
	{
		get => sequenceToken;
		set => sequenceToken = value;
	}

	[JsonConstructor]
	public FdbQueueBatchContainer(
		StreamId streamId,
		List<object> events,
		Dictionary<string, object> requestContext,
		FdbStreamSequenceToken sequenceToken)
		: this(streamId, events, requestContext)
	{
		this.sequenceToken = sequenceToken;
	}

	public FdbQueueBatchContainer(StreamId streamId, List<object> events, Dictionary<string, object> requestContext)
	{
		StreamId = streamId;
		this.events = events ?? throw new ArgumentNullException(nameof(events), "Message contains no events");
		this.requestContext = requestContext;
	}

	public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
	{
		return events.OfType<T>().Select(e => Tuple.Create(e, SequenceToken));
	}

	public bool ImportRequestContext()
	{
		RequestContextExtensions.Import(requestContext);
		return true;
	}

	public override string ToString()
	{
		return $"[FdbQueueBatchContainer:Stream={StreamId},#Items={events.Count}]";
	}
}