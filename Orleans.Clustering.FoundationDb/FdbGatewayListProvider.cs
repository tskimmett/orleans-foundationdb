using System;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Messaging;
using Orleans.Runtime;

namespace Orleans.Clustering.FoundationDb;

public class FdbGatewayListProvider(FdbMembershipTable table, IOptions<GatewayOptions> options) : IGatewayListProvider
{
	public TimeSpan MaxStaleness => options.Value.GatewayListRefreshPeriod;

	public bool IsUpdatable => true;

	public async Task<IList<Uri>> GetGateways()
	{
		if (!table.IsInitialized)
			await table.InitializeMembershipTable(true);

		var data = await table.ReadAll();
		return data.Members
			.Select(pair => pair.Item1)
			.Where(entry => entry.Status == SiloStatus.Active && entry.ProxyPort != 0)
			.Select(entry => SiloAddress.New(entry.SiloAddress.Endpoint.Address, entry.ProxyPort, entry.SiloAddress.Generation).ToGatewayUri())
			.ToList();
	}

	public Task InitializeGatewayListProvider()
	{
		return table.InitializeMembershipTable(true);
	}
}
