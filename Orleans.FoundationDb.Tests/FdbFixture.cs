using FoundationDB.Client;
using FoundationDB.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.FoundationDb.Tests;

public class FdbFixture : IAsyncLifetime
{
	public const string FdbConnectionString = "docker:docker@127.0.0.1:4500";
	readonly string fdbRoot = Guid.NewGuid().ToString();
	readonly FdbDatabaseProvider providerProvider;

	public FdbFixture()
	{
		var options = Options.Create<FdbDatabaseProviderOptions>(new()
		{
			ConnectionOptions = new()
			{
				ConnectionString = FdbConnectionString,
				Root = FdbPath.Absolute(fdbRoot)
			}
		});
		providerProvider = new FdbDatabaseProvider(options);
	}

	public FdbDatabaseProvider Provider => providerProvider;

	public void Dispose()
	{
	}

	public Task InitializeAsync()
	{
		return Task.CompletedTask;
	}

	public Task DisposeAsync()
	{
		// cleanup directory
		return providerProvider.WriteAsync(tx => providerProvider!.Root.RemoveAsync(tx), new());
	}
}