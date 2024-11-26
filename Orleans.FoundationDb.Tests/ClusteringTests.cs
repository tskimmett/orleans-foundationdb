using FoundationDB.Client;
using FoundationDB.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Clustering.FoundationDb;
using Orleans.Messaging;
using UnitTests;
using UnitTests.MembershipTests;

namespace Orleans.FoundationDb.Tests;

public class ClusteringTests : MembershipTableTestsBase, IAsyncLifetime
{
    const string FdbConnectionString = "docker:docker@127.0.0.1:4500";

    string _fdbRoot = Guid.NewGuid().ToString();
    IFdbDatabaseProvider? _fdbProvider;

    public ClusteringTests(ConnectionStringFixture fixture)
        : base(fixture, new(), CreateFilters())
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
            AutoStart = true,
            ConnectionOptions = new()
            {
                ConnectionString = GetConnectionString().Result,
                Root = FdbPath.Absolute(_fdbRoot)
            }
        });
        _fdbProvider = new FdbDatabaseProvider(options);
        return new FdbMembershipTable(_fdbProvider, _clusterOptions);
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
        return _fdbProvider!.WriteAsync(async tx =>
        {
            await _fdbProvider!.Root.RemoveAsync(tx);
        }, new());
    }
}