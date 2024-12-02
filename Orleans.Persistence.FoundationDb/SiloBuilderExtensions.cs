using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration.Overrides;
using Orleans.Hosting;
using Orleans.Runtime.Hosting;

namespace Orleans.Persistence.FoundationDb;

public static class SiloBuilderExtensions
{
	public static ISiloBuilder AddFdbGrainStorage(this ISiloBuilder builder, string name)
	{
		return builder.ConfigureServices(serviceCollection => serviceCollection.AddGrainStorage(name, GrainStorageFactory));
	}

	static FdbGrainStorage GrainStorageFactory(IServiceProvider services, string name)
	{
		var clusterOptions = services.GetProviderClusterOptions(name);
		return ActivatorUtilities.CreateInstance<FdbGrainStorage>(services, name, clusterOptions);
	}
}