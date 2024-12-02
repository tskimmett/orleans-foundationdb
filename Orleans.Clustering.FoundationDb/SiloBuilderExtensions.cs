using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;

namespace Orleans.Clustering.FoundationDb;

public static class SiloBuilderExtensions
{
	public static ISiloBuilder UseFdbClustering(this ISiloBuilder builder)
	{
		return builder.ConfigureServices(serviceCollection => serviceCollection.AddSingleton<IMembershipTable, FdbMembershipTable>());
	}
}