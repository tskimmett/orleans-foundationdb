using FoundationDB.Client;
using FoundationDB.DependencyInjection;
using Microsoft.Extensions.Options;
using UnitTests.StorageTests.Relational;
using UnitTests.StorageTests.Relational.TestDataSets;

namespace Orleans.FoundationDb.Tests;

public class GrainStorageTests : IAsyncLifetime, IClassFixture<CommonFixture>
{
	const string FdbConnectionString = "docker:docker@127.0.0.1:4500";

	readonly string _fdbRoot = Guid.NewGuid().ToString();
	readonly CommonStorageTests _commonStorageTests;
	readonly FdbDatabaseProvider _fdbProvider;

	public GrainStorageTests(CommonFixture commonFixture)
	{
		var options = Options.Create<FdbDatabaseProviderOptions>(new()
		{
			ConnectionOptions = new()
			{
				ConnectionString = FdbConnectionString,
				Root = FdbPath.Absolute(_fdbRoot)
			}
		});
		_fdbProvider = new FdbDatabaseProvider(options);
		_commonStorageTests = new CommonStorageTests(commonFixture.CreateFdbGrainStorage(_fdbProvider).GetAwaiter().GetResult());      
	}

	[SkippableFact]
	[TestCategory("Functional")]
	public async Task WriteInconsistentFailsWithIncosistentStateException()
	{
		await Relational_WriteInconsistentFailsWithIncosistentStateException();
	}

	[SkippableFact]
	[TestCategory("Functional")]
	public async Task WriteRead100StatesInParallel()
	{
		await Relational_WriteReadWriteRead100StatesInParallel();
	}
	internal Task Relational_WriteReadWriteRead100StatesInParallel()
	{
		return _commonStorageTests.PersistenceStorage_WriteReadWriteReadStatesInParallel(nameof(Relational_WriteReadWriteRead100StatesInParallel));
	}

	[SkippableFact]
	[TestCategory("Functional")]
	public async Task WriteReadCyrillic()
	{
		await _commonStorageTests.PersistenceStorage_Relational_WriteReadIdCyrillic();
	}

	[SkippableTheory, ClassData(typeof(StorageDataSet2CyrillicIdsAndGrainNames<string>))]
	[TestCategory("Functional")]
	internal async Task DataSet2_Cyrillic_WriteClearRead(int testNum)
	{
		var (grainType, getGrain, grainState) = StorageDataSet2CyrillicIdsAndGrainNames<string>.GetTestData(testNum);
		await _commonStorageTests.Store_WriteClearRead(grainType, getGrain, grainState);
	}

	[SkippableTheory, ClassData(typeof(StorageDataSetPlain<long>))]
	[TestCategory("Functional")]
	internal async Task PersistenceStorage_StorageDataSetPlain_IntegerKey_WriteClearRead(int testNum)
	{
		var (grainType, getGrain, grainState) = StorageDataSetPlain<long>.GetTestData(testNum);
		await _commonStorageTests.Store_WriteClearRead(grainType, getGrain, grainState);
	}

	[SkippableTheory, ClassData(typeof(StorageDataSetGeneric<Guid, string>))]
	[TestCategory("Functional")]
	internal async Task StorageDataSetGeneric_GuidKey_Generic_WriteClearRead(int testNum)
	{
		var (grainType, getGrain, grainState) = StorageDataSetGeneric<Guid, string>.GetTestData(testNum);
		await _commonStorageTests.Store_WriteClearRead(grainType, getGrain, grainState);
	}

	[SkippableTheory, ClassData(typeof(StorageDataSetGeneric<long, string>))]
	[TestCategory("Functional")]
	internal async Task StorageDataSetGeneric_IntegerKey_Generic_WriteClearRead(int testNum)
	{
		var (grainType, getGrain, grainState) = StorageDataSetGeneric<long, string>.GetTestData(testNum);
		await _commonStorageTests.Store_WriteClearRead(grainType, getGrain, grainState);
	}

	[SkippableTheory, ClassData(typeof(StorageDataSetGeneric<string, string>))]
	[TestCategory("Functional")]
	internal async Task StorageDataSetGeneric_StringKey_Generic_WriteClearRead(int testNum)
	{
		var (grainType, getGrain, grainState) = StorageDataSetGeneric<string, string>.GetTestData(testNum);
		await _commonStorageTests.Store_WriteClearRead(grainType, getGrain, grainState);
	}

	[SkippableTheory, ClassData(typeof(StorageDataSetGeneric<string, string>))]
	[TestCategory("Functional")]
	internal async Task StorageDataSetGeneric_WriteRead(int testNum)
	{
		var (grainType, getGrain, grainState) = StorageDataSetGeneric<string, string>.GetTestData(testNum);
		await _commonStorageTests.Store_WriteRead(grainType, getGrain, grainState);
	}

	[SkippableTheory, ClassData(typeof(StorageDataSetPlain<Guid>))]
	[TestCategory("Functional")]
	internal async Task StorageDataSetPlain_GuidKey_WriteClearRead(int testNum)
	{
		var (grainType, getGrain, grainState) = StorageDataSetPlain<Guid>.GetTestData(testNum);
		await _commonStorageTests.Store_WriteClearRead(grainType, getGrain, grainState);
	}

	[SkippableTheory, ClassData(typeof(StorageDataSetPlain<string>))]
	[TestCategory("Functional")]
	internal async Task StorageDataSetPlain_StringKey_WriteClearRead(int testNum)
	{
		var (grainType, getGrain, grainState) = StorageDataSetPlain<string>.GetTestData(testNum);
		await _commonStorageTests.Store_WriteClearRead(grainType, getGrain, grainState);
	}

	[SkippableFact]
	[TestCategory("Functional")]
	public async Task PersistenceStorage_WriteDuplicateFailsWithInconsistentStateException()
	{
		await Relational_WriteDuplicateFailsWithInconsistentStateException();
	}

	internal async Task Relational_WriteDuplicateFailsWithInconsistentStateException()
	{
		var exception = await _commonStorageTests.PersistenceStorage_WriteDuplicateFailsWithInconsistentStateException();
		CommonStorageUtilities.AssertRelationalInconsistentExceptionMessage(exception.Message);
	}

	internal async Task Relational_WriteInconsistentFailsWithIncosistentStateException()
	{
		var exception = await _commonStorageTests.PersistenceStorage_WriteInconsistentFailsWithInconsistentStateException();
		CommonStorageUtilities.AssertRelationalInconsistentExceptionMessage(exception.Message);
	}

	public Task DisposeAsync()
	{
		// cleanup directory
		return _fdbProvider!.WriteAsync(async tx =>
		{
			await _fdbProvider!.Root.RemoveAsync(tx);
		}, new());
	}

   public Task InitializeAsync()
   {
		return Task.CompletedTask;
   }
}
