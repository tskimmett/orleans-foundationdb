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
	FdbFixture fdbFixture,
	CommonFixture clusterFixture
) :
	ReminderTableTestsBase(fixture, clusterFixture, CreateFilters()),
	IClassFixture<CommonFixture>,
	IClassFixture<FdbFixture>
{
	public const string FdbConnectionString = "docker:docker@127.0.0.1:4500";

	static LoggerFilterOptions CreateFilters()
	{
		LoggerFilterOptions filters = new LoggerFilterOptions();
		filters.AddFilter(nameof(ReminderTests), LogLevel.Trace);
		return filters;
	}

	protected override IReminderTable CreateRemindersTable()
	{
		return new FdbReminderTable(clusterOptions, fdbFixture.Provider, loggerFactory.CreateLogger<FdbReminderTable>());
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