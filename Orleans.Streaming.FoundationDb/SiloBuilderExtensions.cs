using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Configuration;
using Orleans.Hosting;

namespace Orleans.Streaming.FoundationDb;

public static class SiloBuilderExtensions
{
	public static ISiloBuilder AddFdbStreams(this ISiloBuilder builder, string name)
	{
		builder.ConfigureServices(services =>
		{
			services.AddOptions<SimpleQueueCacheOptions>(name);
			services.AddOptions<HashRingStreamQueueMapperOptions>(name)
				.Configure(options => options.TotalQueueCount = 8);
			services.TryAddSingleton<FdbQueueDataAdapter>();
		});
		
		builder.AddPersistentStreams(name, FdbQueueAdapterFactory.Create, null);

		// var configurator = new SiloFdbStreamConfigurator(name,
		// 	configureServicesDelegate => builder.ConfigureServices(configureServicesDelegate));
		// configure?.Invoke(configurator);
		return builder;
	}
}