using Microsoft.Extensions.Logging;
using Orleans.Clustering.FoundationDb;
using Orleans.Messaging;
using UnitTests;
using UnitTests.MembershipTests;

namespace Orleans.FoundationDb.Tests;

public class ClusteringTests(
	ConnectionStringFixture fixture,
	CommonFixture clusterFixture,
	FdbFixture fdbFixture)
	:
		MembershipTableTestsBase(fixture, clusterFixture, CreateFilters()),
		IClassFixture<CommonFixture>,
		IClassFixture<FdbFixture>
{
	static LoggerFilterOptions CreateFilters()
	{
		var filters = new LoggerFilterOptions();
		return filters;
	}

	protected override IMembershipTable CreateMembershipTable(ILogger logger)
	{
		return new FdbMembershipTable(fdbFixture.Provider, _clusterOptions);
	}

	protected override IGatewayListProvider CreateGatewayListProvider(ILogger logger)
	{
		return new FdbGatewayListProvider(
			(FdbMembershipTable)CreateMembershipTable(logger),
			_gatewayOptions);
	}

	protected override Task<string> GetConnectionString() => Task.FromResult("unused");

	[SkippableFact]
	public async Task GetGateways()
	{
		await MembershipTable_GetGateways();
	}

	[SkippableFact]
	public async Task ReadAll_EmptyTable()
	{
		await MembershipTable_ReadAll_EmptyTable();
	}

	[SkippableFact]
	public async Task InsertRow()
	{
		await MembershipTable_InsertRow();
	}

	[SkippableFact]
	public async Task ReadRow_Insert_Read()
	{
		await MembershipTable_ReadRow_Insert_Read();
	}

	[SkippableFact]
	public async Task ReadAll_Insert_ReadAll()
	{
		await MembershipTable_ReadAll_Insert_ReadAll();
	}

	[SkippableFact]
	public async Task UpdateRow()
	{
		await MembershipTable_UpdateRow();
	}

	[SkippableFact]
	public async Task UpdateRowInParallel()
	{
		await MembershipTable_UpdateRowInParallel(false);
	}

	[SkippableFact]
	public async Task UpdateIAmAlive()
	{
		await MembershipTable_UpdateIAmAlive();
	}

	[SkippableFact]
	public async Task CleanupDefunctSiloEntries()
	{
		await MembershipTable_CleanupDefunctSiloEntries(false);
	}
}