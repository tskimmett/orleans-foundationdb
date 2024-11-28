using FoundationDB.Client;
using FoundationDB.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Clustering.FoundationDb;
using Orleans.Messaging;
using UnitTests;
using UnitTests.MembershipTests;

namespace Orleans.FoundationDb.Tests;

public class ClusteringTests : MembershipTableTestsBase, IClassFixture<CommonFixture>, IAsyncLifetime
{
	const string FdbConnectionString = "docker:docker@127.0.0.1:4500";

	// each test run will be scoped to a unique fdb directory
	readonly string fdbRoot = Guid.NewGuid().ToString();
	IFdbDatabaseProvider? fdbProvider;

	public ClusteringTests(ConnectionStringFixture fixture, CommonFixture clusterFixture)
		 : base(fixture, clusterFixture, CreateFilters())
	{ }

	private static LoggerFilterOptions CreateFilters()
	{
		var filters = new LoggerFilterOptions();
		return filters;
	}

	protected override IMembershipTable CreateMembershipTable(ILogger logger)
	{
		var options = Options.Create<FdbDatabaseProviderOptions>(new()
		{
			ConnectionOptions = new()
			{
				ConnectionString = GetConnectionString().Result,
				Root = FdbPath.Absolute(fdbRoot)
			}
		});
		fdbProvider = new FdbDatabaseProvider(options);
		return new FdbMembershipTable(fdbProvider, _clusterOptions);
	}

	protected override IGatewayListProvider CreateGatewayListProvider(ILogger logger)
	{
		return new FdbGatewayListProvider(
			 (FdbMembershipTable)CreateMembershipTable(logger),
			 _gatewayOptions);
	}

	protected override Task<string> GetConnectionString()
		 => Task.FromResult(FdbConnectionString);

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

	public Task InitializeAsync()
	{
		return Task.CompletedTask;
	}

	public Task DisposeAsync()
	{
		// cleanup directory
		return fdbProvider!.WriteAsync(async tx =>
		{
			await fdbProvider!.Root.RemoveAsync(tx);
		}, new());
	}
}