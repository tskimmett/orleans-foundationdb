using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Streaming.FoundationDb
{
	public class FdbStreamFailureHandler(ILogger<FdbStreamFailureHandler> logger) : IStreamFailureHandler
	{
		public bool ShouldFaultSubsriptionOnError => true;

		public Task OnDeliveryFailure(GuidId subscriptionId, string streamProviderName, StreamId streamIdentity, StreamSequenceToken sequenceToken)
		{
			logger.LogError("Delivery failure for subscription {SubscriptionId} on stream {StreamId} with token {Token}", subscriptionId, streamIdentity, sequenceToken);
			return Task.CompletedTask;
		}

		public Task OnSubscriptionFailure(GuidId subscriptionId, string streamProviderName, StreamId streamIdentity, StreamSequenceToken sequenceToken)
		{
			logger.LogError("Subscription failure for subscription {SubscriptionId} on stream {StreamId} with token {Token}", subscriptionId, streamIdentity, sequenceToken);
			return Task.CompletedTask;
		}
	}
}