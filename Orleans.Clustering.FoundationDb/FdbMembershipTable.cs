using System.Text.Json;
using Doxense.Collections.Tuples;
using FoundationDB.Client;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Runtime;

namespace Orleans.Clustering.FoundationDb;

public class FdbMembershipTable : IMembershipTable
{
    const string DirName = "orleans-clustering";
    readonly ClusterOptions _clusterOptions;
    readonly IFdbDatabaseProvider _fdb;

    public FdbMembershipTable(IFdbDatabaseProvider fdb, IOptions<ClusterOptions> clusterOptions)
    {
        _fdb = fdb;
        _clusterOptions = clusterOptions.Value;
    }

    string ClusterId => _clusterOptions.ClusterId;

    Task WriteAsync(Func<IFdbTransaction, FdbDirectorySubspace, Task> write)
        => _fdb.WriteAsync(async tx => await write(tx, await GetDirectory(tx)), new());

    Task<T> ReadWriteAsync<T>(Func<IFdbTransaction, FdbDirectorySubspace, Task<T>> readWrite)
        => _fdb.ReadWriteAsync(async tx => await readWrite(tx, await GetDirectory(tx)), new());

    Task WriteAsync(Action<IFdbTransaction, FdbDirectorySubspace> write)
        => _fdb.WriteAsync(async tx => write(tx, await GetDirectory(tx)), new());

    Task<T> ReadAsync<T>(Func<IFdbReadOnlyTransaction, FdbDirectorySubspace, Task<T>> read)
        => _fdb.ReadAsync(async tx => await read(tx, await GetDirectory(tx)), new());

    async Task<int> GetVersion(IFdbReadOnlyTransaction tx)
    {
        var res = await tx.GetAsync(VersionKey(await GetDirectory(tx)));
        var dbVersion = await JsonSerializer.DeserializeAsync<TableVersion>(res.ToStream());
        return dbVersion!.Version;
    }

    public Task InitializeMembershipTable(bool tryInitTableVersion)
    {
        return WriteAsync((t, dir) =>
        {
            if (tryInitTableVersion)
            {
                t.Set(VersionKey(dir), JsonSerializer.SerializeToUtf8Bytes(new TableVersion(0, "0")));
            }
        });
    }

    public Task CleanupDefunctSiloEntries(DateTimeOffset beforeDate)
    {
        return WriteAsync(async (tx, dir) =>
        {
            var entries = (await tx.GetRangeAsync(dir.PackRange((ClusterId, "member")), null))
                .Select(e => Deserialize<MembershipEntry>(e.Value))
                .Where(member => member.Status != SiloStatus.Active
                    && member.StartTime < beforeDate
                    && member.IAmAliveTime < beforeDate);

            foreach (var defunct in entries)
                tx.Clear(MemberKey(dir, defunct));
        });
    }

    public Task DeleteMembershipTableEntries(string clusterId)
    {
        return WriteAsync((t, dir) => t.ClearRange(dir.PackRange(STuple.Create(clusterId))));
    }

    public Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
    {
        var data = JsonSerializer.SerializeToUtf8Bytes(entry);
        return ReadWriteAsync(async (tx, dir) =>
        {
            var res = await tx.GetAsync(VersionKey(dir));

            if (await GetVersion(tx) != tableVersion.Version - 1)
                return false;

            var entryKey = MemberKey(dir, entry);
            // Entry already exists
            if (await tx.GetAsync(entryKey) != Slice.Nil)
                return false;

            tx.Set(VersionKey(dir), JsonSerializer.SerializeToUtf8Bytes(tableVersion));
            tx.Set(entryKey, data);
            return true;
        });
    }

    Slice MemberKey(FdbDirectorySubspace dir, MembershipEntry entry)
    {
        return dir.Encode(ClusterId, "member", entry.SiloAddress.ToString());
    }

    Slice MemberEtagKey(FdbDirectorySubspace dir, MembershipEntry entry)
    {
        return dir.Encode(ClusterId, "member", entry.SiloAddress.ToString(), "etag");
    }

    public async Task<MembershipTableData> ReadAll()
    {
        var res = await ReadAsync(async (tx, dir) =>
        {
            var versionTask = GetVersion(tx);
            var rangeTask = tx.GetRangeAsync(dir.PackRange((ClusterId, "member")), null);
            await Task.WhenAll(versionTask, rangeTask);
            return (version: versionTask.Result, members: rangeTask.Result.Select(e => e.Value));
        });

        var memberList = res.members
            .Chunk(2)   // collate [data, etag]
            .Select(member => Tuple.Create(Deserialize<MembershipEntry>(member.First()), member.Last().ToUuid80().ToString()))
            .ToList();
        return new MembershipTableData(memberList, new TableVersion(res.version, res.version.ToString()));
    }

    static T Deserialize<T>(Slice data) => JsonSerializer.Deserialize<T>(data.ToStream())!;

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
        var aliveTime = entry.IAmAliveTime;
        return WriteAsync(async (tx, dir) =>
        {
            var entryKey = MemberKey(dir, entry);
            var item = Deserialize<MembershipEntry>(await tx.GetAsync(entryKey));
            item!.IAmAliveTime = aliveTime;
            tx.Set(entryKey, JsonSerializer.SerializeToUtf8Bytes(item));
        });
    }

    public Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
    {
        return ReadWriteAsync(async (tx, dir) =>
        {
            var etagKey = MemberEtagKey(dir, entry);
            var dbEtag = await tx.GetAsync(etagKey);
            // Cannot update nonexistent entry
            if (dbEtag == Slice.Nil)
                return false;

            if (etag != dbEtag.ToUuid80().ToString())
                return false;

            tx.Set(etagKey, tx.CreateVersionStamp().ToSlice());
            tx.Set(MemberKey(dir, entry), JsonSerializer.SerializeToUtf8Bytes(entry));
            return true;
        });
    }

    async ValueTask<FdbDirectorySubspace> GetDirectory(IFdbReadOnlyTransaction t)
    {
        var subspace = await _fdb.Root[DirName].Resolve(t);
        return subspace!;
    }

    Slice VersionKey(FdbDirectorySubspace dir)
    {
        return dir.Pack((ClusterId, "version"));
    }
}