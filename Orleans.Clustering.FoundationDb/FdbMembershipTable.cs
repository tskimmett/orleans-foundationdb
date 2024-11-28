using Doxense.Collections.Tuples;
using FoundationDB.Client;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.Configuration;
using Orleans.Runtime;

namespace Orleans.Clustering.FoundationDb;

public class FdbMembershipTable(IFdbDatabaseProvider fdb, IOptions<ClusterOptions> clusterOptions)
	: IMembershipTable
{
	const string DirName = "orleans-clustering";
	readonly ClusterOptions clusterOptions = clusterOptions.Value;
	readonly JsonSerializerSettings jsonSerializerSettings = JsonSettings.JsonSerializerSettings;

	string ClusterId => clusterOptions.ClusterId;

	public bool IsInitialized { get; private set; }

	async Task<int> GetVersion(IFdbReadOnlyTransaction tx)
	{
		var res = await tx.GetAsync(VersionKey(await GetDirectory(tx)));
		var dbVersion = Deserialize<TableVersion>(res);
		return dbVersion!.Version;
	}

	public async Task InitializeMembershipTable(bool tryInitTableVersion)
	{
		if (tryInitTableVersion)
		{
			await fdb.WriteAsync(async tx =>
			{
				var dir = await EnsureDirectory(tx);
				tx.Set(VersionKey(dir), Serialize(new TableVersion(0, "0")));
			}, new());
		}

		IsInitialized = true;
	}

	public Task CleanupDefunctSiloEntries(DateTimeOffset beforeDate)
	{
		var etagSlice = Slice.FromString("etag");
		return WriteAsync(async (tx, dir) =>
		{
			var entries = (await tx.GetRangeAsync(dir.PackRange((ClusterId, "member")), null))
				.Where(e => dir.Unpack(e.Key).Last<string>() != "etag")
				.Select(e => Deserialize<MembershipEntry>(e.Value))
				.Where(member => member.Status != SiloStatus.Active
				                 && member.StartTime < beforeDate
				                 && member.IAmAliveTime < beforeDate);

			foreach (var defunct in entries)
			{
				tx.Clear(MemberKey(dir, defunct));
				tx.Clear(MemberEtagKey(dir, defunct));
			}
		});
	}

	public Task DeleteMembershipTableEntries(string clusterId)
	{
		return WriteAsync((t, dir) =>
		{
			if (dir != null)
				t.ClearRange(dir.PackRange(STuple.Create(clusterId)));
		});
	}

	public async Task<MembershipTableData> ReadAll()
	{
		var (version, members) = await ReadAsync(async (tx, dir) =>
		{
			var versionTask = tx.GetAsync(VersionKey(dir));
			var rangeTask = tx.GetRange(dir.PackRange((ClusterId, "member"))).ToListAsync();
			await Task.WhenAll(versionTask, rangeTask);
			return (version: versionTask.Result, members: rangeTask.Result);
		});

		var memberList = members
			.Chunk(2) // collate [data, etag]
			.Select(member => Tuple.Create(Deserialize<MembershipEntry>(member.First().Value),
				member.Last().Value.ToUuid80().ToString()))
			.ToList();
		return new MembershipTableData(memberList, Deserialize<TableVersion>(version));
	}

	T Deserialize<T>(Slice data) => JsonConvert.DeserializeObject<T>(data.ToStringUtf8()!, jsonSerializerSettings)!;

	Slice Serialize<T>(T item) => Slice.FromStringUtf8(JsonConvert.SerializeObject(item, jsonSerializerSettings));

	public async Task<MembershipTableData> ReadRow(SiloAddress key)
	{
		var (version, data, etag) = await ReadAsync<(int version, Slice data, Slice etag)>(async (tx, dir) =>
		{
			var memberKey = (ClusterId, "member", key.ToString()).ToSTuple();
			var data = tx.GetAsync(dir.Pack(memberKey));
			var etag = tx.GetAsync(dir.Pack(memberKey.Append("etag")));
			var version = GetVersion(tx);
			return (await version, await data, await etag);
		});

		return new MembershipTableData(
			Tuple.Create(Deserialize<MembershipEntry>(data), etag.ToUuid80().ToString()),
			new TableVersion(version, version.ToString())
		);
	}

	public Task UpdateIAmAlive(MembershipEntry entry)
	{
		return WriteAsync(async (tx, dir) =>
		{
			var entryKey = MemberKey(dir, entry);
			var item = Deserialize<MembershipEntry>(await tx.GetAsync(entryKey));
			item!.IAmAliveTime = entry.IAmAliveTime;
			tx.Set(entryKey, Serialize(item));
		});
	}

	public Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
	{
		var versionData = Serialize(tableVersion);
		return ReadWriteAsync(async (tx, dir) =>
		{
			if (await GetVersion(tx) != tableVersion.Version - 1)
				return false;

			var entryKey = MemberKey(dir, entry);
			// Entry already exists
			if (await tx.GetAsync(entryKey) != Slice.Nil)
				return false;

			tx.Set(VersionKey(dir), versionData);
			tx.Set(entryKey, Serialize(entry));
			tx.SetVersionStampedValue(MemberEtagKey(dir, entry), tx.CreateVersionStamp().ToSlice());
			return true;
		});
	}


	public Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
	{
		return ReadWriteAsync(async (tx, dir) =>
		{
			if (await GetVersion(tx) != tableVersion.Version - 1)
				return false;

			var etagKey = MemberEtagKey(dir, entry);
			var dbEtag = await tx.GetAsync(etagKey);
			// Cannot update nonexistent entry
			if (dbEtag == Slice.Nil)
				return false;

			if (etag != dbEtag.ToUuid80().ToString())
				return false;

			tx.Set(VersionKey(dir), Serialize(tableVersion));
			tx.Set(MemberKey(dir, entry), Serialize(entry));
			tx.SetVersionStampedValue(etagKey, tx.CreateVersionStamp().ToSlice());
			return true;
		});
	}

	async ValueTask<FdbDirectorySubspace> GetDirectory(IFdbReadOnlyTransaction t)
	{
		var subspace = await fdb.Root[DirName].Resolve(t);
		return subspace!;
	}

	async ValueTask<FdbDirectorySubspace> EnsureDirectory(IFdbTransaction t)
	{
		var subspace = await fdb.Root[DirName].CreateOrOpenAsync(t);
		return subspace!;
	}

	Slice VersionKey(FdbDirectorySubspace dir)
	{
		return dir.Pack((ClusterId, "version"));
	}

	Slice MemberKey(FdbDirectorySubspace dir, MembershipEntry entry)
	{
		return dir.Encode(ClusterId, "member", entry.SiloAddress.ToString());
	}

	Slice MemberEtagKey(FdbDirectorySubspace dir, MembershipEntry entry)
	{
		return dir.Encode(ClusterId, "member", entry.SiloAddress.ToString(), "etag");
	}

	Task WriteAsync(Func<IFdbTransaction, FdbDirectorySubspace, Task> write)
		=> fdb.WriteAsync(async tx => await write(tx, await GetDirectory(tx)), new());

	Task<T> ReadWriteAsync<T>(Func<IFdbTransaction, FdbDirectorySubspace, Task<T>> readWrite)
		=> fdb.ReadWriteAsync(async tx => await readWrite(tx, await GetDirectory(tx)), new());

	Task WriteAsync(Action<IFdbTransaction, FdbDirectorySubspace> write)
		=> fdb.WriteAsync(async tx => write(tx, await GetDirectory(tx)), new());

	Task<T> ReadAsync<T>(Func<IFdbReadOnlyTransaction, FdbDirectorySubspace, Task<T>> read)
		=> fdb.ReadAsync(async tx => await read(tx, await GetDirectory(tx)), new());
}