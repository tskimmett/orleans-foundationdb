using System.Diagnostics;
using FoundationDB.Client;
using FoundationDB.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Persistence.FoundationDb;
using Orleans.Streaming.FoundationDb;
using Orleans.TestingHost;
using TestExtensions;
using UnitTests.Streaming;
using UnitTests.StreamingTests;
using Xunit.Abstractions;

namespace Orleans.FoundationDb.Tests
{
	public class FdbStreamingTests(ITestOutputHelper output) : TestClusterPerTest
	{
		public const string FdbStreamProviderName = "FdbQueueProvider";
		public const string SmsStreamProviderName = StreamTestsConstants.SMS_STREAM_PROVIDER_NAME;
		SingleStreamTestRunner runner = null!;
		ITestOutputHelper output = output;

		protected override void ConfigureTestCluster(TestClusterBuilder builder)
		{
			builder.AddSiloBuilderConfigurator<SiloBuilderConfigurator>();
			builder.AddClientBuilderConfigurator<MyClientBuilderConfigurator>();
		}

		class MyClientBuilderConfigurator : IClientBuilderConfigurator
		{
			public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
			{
				clientBuilder.ConfigureServices(svc =>
				{
					svc.AddSingleton<IFdbDatabaseProvider>(provider =>
					{
						var clusterOptions = provider.GetRequiredService<IOptions<ClusterOptions>>();
						return CreateFdbProvider(clusterOptions.Value.ClusterId);
					});
				});
				clientBuilder.AddFdbStreams(FdbStreamProviderName);
			}
		}

		class SiloBuilderConfigurator : ISiloConfigurator
		{
			public void Configure(ISiloBuilder hostBuilder)
			{
				hostBuilder.ConfigureServices(svc =>
				{
					svc.AddSingleton<IFdbDatabaseProvider>(provider =>
					{
						var clusterOptions = provider.GetRequiredService<IOptions<ClusterOptions>>();
						return CreateFdbProvider(clusterOptions.Value.ClusterId);
					});
				});
				hostBuilder
					.AddFdbGrainStorage("FdbStore")
					.AddFdbGrainStorage("PubSubStore")
					.AddMemoryGrainStorage("MemoryStore")
					.AddFdbStreams(FdbStreamProviderName);
			}
		}

		static IFdbDatabaseProvider CreateFdbProvider(string clusterId)
		{
			var options = Options.Create<FdbDatabaseProviderOptions>(new()
			{
				ConnectionOptions = new()
				{
					ConnectionString = FdbFixture.FdbConnectionString,
					Root = FdbPath.Absolute(clusterId)
				}
			});
			return new FdbDatabaseProvider(options);
		}

		public override async Task InitializeAsync()
		{
			await base.InitializeAsync();
			runner = new SingleStreamTestRunner(InternalClient, FdbStreamProviderName);
		}

		public override async Task DisposeAsync()
		{
			var clusterOptions = Client.ServiceProvider.GetRequiredService<IOptions<ClusterOptions>>();
			await base.DisposeAsync();
			var fdb = CreateFdbProvider(clusterOptions.Value.ClusterId);
			await fdb.WriteAsync(tx => fdb!.Root.RemoveAsync(tx), new());
		}

		////------------------------ One to One ----------------------//

		[SkippableFact, TestCategory("Functional")]
		public async Task FDB_01_OneProducerGrainOneConsumerGrain()
		{
			await runner.StreamTest_01_OneProducerGrainOneConsumerGrain();
		}

		[SkippableFact, TestCategory("Functional")]
		public async Task FDB_02_OneProducerGrainOneConsumerClient()
		{
			await runner.StreamTest_02_OneProducerGrainOneConsumerClient();
		}

		[SkippableFact, TestCategory("Functional")]
		public async Task FDB_03_OneProducerClientOneConsumerGrain()
		{
			await runner.StreamTest_03_OneProducerClientOneConsumerGrain();
		}

		[SkippableFact, TestCategory("Functional")]
		public async Task FDB_04_OneProducerClientOneConsumerClient()
		{
			await runner.StreamTest_04_OneProducerClientOneConsumerClient();
		}

		//------------------------ MANY to Many different grains ----------------------//

		[SkippableFact, TestCategory("Functional")]
		public async Task FDB_05_ManyDifferent_ManyProducerGrainsManyConsumerGrains()
		{
			await runner.StreamTest_05_ManyDifferent_ManyProducerGrainsManyConsumerGrains();
		}

		[SkippableFact, TestCategory("Functional")]
		public async Task FDB_06_ManyDifferent_ManyProducerGrainManyConsumerClients()
		{
			await runner.StreamTest_06_ManyDifferent_ManyProducerGrainManyConsumerClients();
		}

		[SkippableFact(Skip = "https://github.com/dotnet/orleans/issues/5648"), TestCategory("Functional")]
		public async Task FDB_07_ManyDifferent_ManyProducerClientsManyConsumerGrains()
		{
			await runner.StreamTest_07_ManyDifferent_ManyProducerClientsManyConsumerGrains();
		}

		[SkippableFact, TestCategory("Functional")]
		public async Task FDB_08_ManyDifferent_ManyProducerClientsManyConsumerClients()
		{
			await runner.StreamTest_08_ManyDifferent_ManyProducerClientsManyConsumerClients();
		}

		//------------------------ MANY to Many Same grains ----------------------//
		[SkippableFact, TestCategory("Functional")]
		public async Task FDB_09_ManySame_ManyProducerGrainsManyConsumerGrains()
		{
			await runner.StreamTest_09_ManySame_ManyProducerGrainsManyConsumerGrains();
		}

		[SkippableFact, TestCategory("Functional")]
		public async Task FDB_10_ManySame_ManyConsumerGrainsManyProducerGrains()
		{
			await runner.StreamTest_10_ManySame_ManyConsumerGrainsManyProducerGrains();
		}

		[SkippableFact, TestCategory("Functional")]
		public async Task FDB_11_ManySame_ManyProducerGrainsManyConsumerClients()
		{
			await runner.StreamTest_11_ManySame_ManyProducerGrainsManyConsumerClients();
		}

		[SkippableFact, TestCategory("Functional")]
		public async Task FDB_12_ManySame_ManyProducerClientsManyConsumerGrains()
		{
			await runner.StreamTest_12_ManySame_ManyProducerClientsManyConsumerGrains();
		}

		//------------------------ MANY to Many producer consumer same grain ----------------------//

		[SkippableFact, TestCategory("Functional")]
		public async Task FDB_13_SameGrain_ConsumerFirstProducerLater()
		{
			await runner.StreamTest_13_SameGrain_ConsumerFirstProducerLater(false);
		}

		[SkippableFact, TestCategory("Functional")]
		public async Task FDB_14_SameGrain_ProducerFirstConsumerLater()
		{
			await runner.StreamTest_14_SameGrain_ProducerFirstConsumerLater(false);
		}

		//----------------------------------------------//

		[SkippableFact, TestCategory("Functional")]
		public async Task FDB_15_ConsumeAtProducersRequest()
		{
			await runner.StreamTest_15_ConsumeAtProducersRequest();
		}

		[SkippableFact, TestCategory("Functional")]
		public async Task FDB_16_MultipleStreams_ManyDifferent_ManyProducerGrainsManyConsumerGrains()
		{
			var multiRunner = new MultipleStreamsTestRunner(InternalClient, FdbStreamProviderName, 16, false);
			await multiRunner.StreamTest_MultipleStreams_ManyDifferent_ManyProducerGrainsManyConsumerGrains();
		}

		[SkippableFact, TestCategory("Functional")]
		public async Task FDB_17_MultipleStreams_1J_ManyProducerGrainsManyConsumerGrains()
		{
			var multiRunner = new MultipleStreamsTestRunner(InternalClient, FdbStreamProviderName, 17, false);
			await multiRunner.StreamTest_MultipleStreams_ManyDifferent_ManyProducerGrainsManyConsumerGrains(
				HostedCluster.StartAdditionalSilo);
		}

		//[SkippableFact, TestCategory("BVT")]
		/*public async Task FDB_18_MultipleStreams_1J_1F_ManyProducerGrainsManyConsumerGrains()
		{
		    var multiRunner = new MultipleStreamsTestRunner(this.InternalClient, SingleStreamTestRunner.FDB_STREAM_PROVIDER_NAME, 18, false);
		    await multiRunner.StreamTest_MultipleStreams_ManyDifferent_ManyProducerGrainsManyConsumerGrains(
		        this.HostedCluster.StartAdditionalSilo,
		        this.HostedCluster.StopSilo);
		}*/

		[SkippableFact]
		public async Task FDB_19_ConsumerImplicitlySubscribedToProducerClient()
		{
			// todo: currently, the Azure queue queue adaptor doesn't support namespaces, so this test will fail.
			await runner.StreamTest_19_ConsumerImplicitlySubscribedToProducerClient();
		}

		[SkippableFact]
		public async Task FDB_20_ConsumerImplicitlySubscribedToProducerGrain()
		{
			// todo: currently, the Azure queue queue adaptor doesn't support namespaces, so this test will fail.
			await runner.StreamTest_20_ConsumerImplicitlySubscribedToProducerGrain();
		}

		[SkippableFact(Skip = "Ignored"), TestCategory("Failures")]
		public async Task FDB_21_GenericConsumerImplicitlySubscribedToProducerGrain()
		{
			// todo: currently, the Azure queue queue adaptor doesn't support namespaces, so this test will fail.
			await runner.StreamTest_21_GenericConsumerImplicitlySubscribedToProducerGrain();
		}
	}
}