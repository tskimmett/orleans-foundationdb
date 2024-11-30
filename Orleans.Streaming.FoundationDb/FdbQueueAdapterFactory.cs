using FoundationDB.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace Orleans.Streaming.FoundationDb;

public class FdbQueueAdapterFactory(
	string providerName,
	IFdbDatabaseProvider fdb,
	FdbQueueDataAdapter dataAdapter,
	IStreamFailureHandler streamFailureHandler,
	SimpleQueueCacheOptions cacheOptions,
	HashRingStreamQueueMapperOptions queueMapperOptions,
	ILoggerFactory loggerFactory
) : IQueueAdapterFactory
{
	readonly HashRingBasedStreamQueueMapper queueMapper = new(queueMapperOptions, providerName);
	readonly SimpleQueueAdapterCache cache = new(cacheOptions, providerName, loggerFactory);

	public static IQueueAdapterFactory Create(IServiceProvider services, string providerName)
	{
		var fdb = services.GetRequiredService<IFdbDatabaseProvider>();
		var loggerFactory = services.GetRequiredService<ILoggerFactory>();
		var simpleQueueCacheOptions = services.GetOptionsByName<SimpleQueueCacheOptions>(providerName);
		var hashRingStreamQueueMapperOptions = services.GetOptionsByName<HashRingStreamQueueMapperOptions>(providerName);
		var dataAdapter = services.GetKeyedService<FdbQueueDataAdapter>(providerName)
		                  ?? services.GetService<FdbQueueDataAdapter>();
		var streamFailureHandler = new FdbStreamFailureHandler(loggerFactory.CreateLogger<FdbStreamFailureHandler>());
		return new FdbQueueAdapterFactory(
			providerName,
			fdb,
			dataAdapter!,
			streamFailureHandler,
			simpleQueueCacheOptions,
			hashRingStreamQueueMapperOptions,
			loggerFactory);
	}

	public Task<IQueueAdapter> CreateAdapter()
	{
		return Task.FromResult<IQueueAdapter>(new FdbQueueAdapter(
			providerName,
			fdb,
			dataAdapter,
			queueMapper,
			loggerFactory));
	}

	public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
		=> Task.FromResult(streamFailureHandler);

	public IQueueAdapterCache GetQueueAdapterCache() => cache;

	public IStreamQueueMapper GetStreamQueueMapper() => queueMapper;
}