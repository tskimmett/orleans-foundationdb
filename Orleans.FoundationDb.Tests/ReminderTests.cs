using FoundationDB.Client;
using FoundationDB.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Reminders.FoundationDb;
using UnitTests;
using UnitTests.RemindersTest;

namespace Orleans.FoundationDb.Tests;

public class ReminderTests(
	ConnectionStringFixture fixture,
	CommonFixture clusterFixture
) : ReminderTableTestsBase(fixture, clusterFixture, CreateFilters()), IClassFixture<CommonFixture>
{
	const string FdbConnectionString = "docker:docker@127.0.0.1:4500";

	// each test run will be scoped to a unique fdb directory
	readonly string _fdbRoot = Guid.NewGuid().ToString();
	FdbDatabaseProvider _fdbProvider;

	static LoggerFilterOptions CreateFilters()
	{
		LoggerFilterOptions filters = new LoggerFilterOptions();
		filters.AddFilter(nameof(ReminderTests), LogLevel.Trace);
		return filters;
	}

	protected override IReminderTable CreateRemindersTable()
	{
		var options = Options.Create<FdbDatabaseProviderOptions>(new()
		{
			ConnectionOptions = new()
			{
				ConnectionString = GetConnectionString().Result,
				Root = FdbPath.Absolute(_fdbRoot)
			}
		});
		_fdbProvider = new FdbDatabaseProvider(options);
		return new FdbReminderTable(clusterOptions, _fdbProvider, loggerFactory.CreateLogger<FdbReminderTable>());
	}

	protected override Task<string> GetConnectionString() => Task.FromResult(FdbConnectionString);

	[SkippableFact]
	public async Task RemindersTable_Redis_RemindersRange()
	{
		await RemindersRange(iterations: 50);
	}

	[SkippableFact]
	public async Task RemindersTable_Redis_RemindersParallelUpsert()
	{
		await RemindersParallelUpsert();
	}

	[SkippableFact]
	public async Task RemindersTable_Redis_ReminderSimple()
	{
		await ReminderSimple();
	}
}