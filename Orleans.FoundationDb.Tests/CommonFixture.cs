using FoundationDB.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Persistence.FoundationDb;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;
using TestExtensions;

namespace Orleans.FoundationDb.Tests;

public class CommonFixture : TestEnvironmentFixture
{
	IProviderRuntime DefaultProviderRuntime { get; }

	public CommonFixture()
	{
		Client.ServiceProvider.GetRequiredService<IOptions<ClusterOptions>>();
		DefaultProviderRuntime = new ClientProviderRuntime(
			InternalGrainFactory,
			Client.ServiceProvider,
			Client.ServiceProvider.GetRequiredService<ClientGrainContext>()
		);
	}

	public async Task<IGrainStorage> CreateFdbGrainStorage(IFdbDatabaseProvider fdb)
	{
		IGrainStorageSerializer grainStorageSerializer = new OrleansGrainStorageSerializer(DefaultProviderRuntime.ServiceProvider.GetService<Serializer>());

		var clusterOptions = new ClusterOptions()
		{
			ServiceId = Guid.NewGuid().ToString()
		};

		var storageProvider = new FdbGrainStorage(
			string.Empty,
			fdb,
			grainStorageSerializer,
			Options.Create(clusterOptions),
			DefaultProviderRuntime.ServiceProvider.GetRequiredService<ILogger<FdbGrainStorage>>()
		);
		ISiloLifecycleSubject siloLifeCycle = new SiloLifecycleSubject(NullLoggerFactory.Instance.CreateLogger<SiloLifecycleSubject>());
		storageProvider.Participate(siloLifeCycle);
		await siloLifeCycle.OnStart(CancellationToken.None);
		return storageProvider;
	}
}