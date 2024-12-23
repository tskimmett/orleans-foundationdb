using Orleans.Streams;

namespace Orleans.Streaming.FoundationDb;

[GenerateSerializer]
public sealed class FdbStreamSequenceToken : StreamSequenceToken
{
	public static readonly FdbStreamSequenceToken Identity = new(0, 0);

	[Id(0)]
	public override long SequenceNumber { get; protected set; }

	[Id(1)]
	public override int EventIndex { get; protected set; }

	public FdbStreamSequenceToken(VersionStamp fdbStamp)
	{
		SequenceNumber = (long)fdbStamp.TransactionVersion;
		EventIndex = fdbStamp.TransactionOrder;
	}

	public FdbStreamSequenceToken(long sequenceNumber, int eventIndex)
	{
		SequenceNumber = sequenceNumber;
		EventIndex = eventIndex;
	}

	public override int CompareTo(StreamSequenceToken other)
	{
		if (this == Identity || other == Identity)
			return 0;
		
		return other switch
		{
			null => throw new ArgumentNullException(nameof(other)),
			FdbStreamSequenceToken token when SequenceNumber == token.SequenceNumber => EventIndex.CompareTo(token.EventIndex),
			FdbStreamSequenceToken token => SequenceNumber.CompareTo(token.SequenceNumber),
			_ => throw new ArgumentException("Invalid token type", nameof(other))
		};
	}

	public override bool Equals(StreamSequenceToken? other)
	{
		var token = other as FdbStreamSequenceToken;
		return token != null && SequenceNumber == token.SequenceNumber && EventIndex == token.EventIndex;
	}

	public VersionStamp ToVersionStamp() => VersionStamp.Complete((ulong)SequenceNumber, (ushort)EventIndex);

	public override string ToString()
	{
		return $"{SequenceNumber}-{EventIndex}";
	}
}