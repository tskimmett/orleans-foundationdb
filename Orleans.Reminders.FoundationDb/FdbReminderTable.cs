using FoundationDB.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.Concurrency;
using Orleans.Configuration;

namespace Orleans.Reminders.FoundationDb;

public class FdbReminderTable(
	IOptions<ClusterOptions> clusterOptions,
	IFdbDatabaseProvider fdb,
	ILogger<FdbReminderTable> logger
) : IReminderTable
{
	const string DirName = "orleans-reminders";
	readonly ClusterOptions _clusterOptions = clusterOptions.Value;

	readonly JsonSerializerSettings _jsonSettings = new()
	{
		DateFormatHandling = DateFormatHandling.IsoDateFormat,
		DefaultValueHandling = DefaultValueHandling.Ignore,
		MissingMemberHandling = MissingMemberHandling.Ignore,
		NullValueHandling = NullValueHandling.Ignore,
	};

	public Task StartAsync(CancellationToken ct)
	{
		// ensure grain state directory exists
		return fdb.WriteAsync(tx => fdb.Root[DirName].CreateOrOpenAsync(tx), ct);
	}

	public async Task<ReminderTableData> ReadRows(GrainId grainId)
	{
		try
		{
			var rows = await ReadAsync(async (tx, dir) =>
			{
				var list = await tx.GetRange(dir.EncodeRange(grainId.GetUniformHashCode())).ToListAsync();
				return list
					.Where(row => dir.DecodeLast<string>(row.Key) == "data")
					.Select(row => row.Value);
			});
			return new ReminderTableData(rows.Select(Deserialize<ReminderEntry>));
		}
		catch (Exception ex)
		{
			logger.LogError(ex, "Failure reading reminders for grain: {GrainId}", grainId);
			WrappedException.CreateAndRethrow(ex);
			throw;
		}
	}

	public async Task<ReminderTableData> ReadRows(uint begin, uint end)
	{
		try
		{
			var rows = await ReadAsync(async (tx, dir) =>
			{
				IEnumerable<KeyValuePair<Slice, Slice>> list;
				if (begin < end)
				{
					list = await tx.GetRange(
						KeySelector.FirstGreaterThan(dir.Encode(begin)),
						KeySelector.LastLessOrEqual(dir.Encode(end))
					).ToListAsync();
				}
				else
				{
					var lower = tx.GetRange(
						KeySelector.FirstGreaterOrEqual(dir.Encode(uint.MinValue)),
						KeySelector.LastLessOrEqual(dir.Encode(end))
					).ToListAsync();
					var upper = tx.GetRange(
						KeySelector.FirstGreaterThan(dir.Encode(begin)),
						KeySelector.LastLessOrEqual(dir.Encode(uint.MaxValue))
					).ToListAsync();
					list = (await lower).Concat(await upper);
				}

				return list
					.Where(row => dir.DecodeLast<string>(row.Key) == "data")
					.Select(row => row.Value);
			});
			return new ReminderTableData(rows.Select(Deserialize<ReminderEntry>));
		}
		catch (Exception ex)
		{
			logger.LogError(ex, "Failure reading reminder range: {Start} - {End}", begin, end);
			WrappedException.CreateAndRethrow(ex);
			throw;
		}
	}

	public async Task<ReminderEntry> ReadRow(GrainId grainId, string reminderName)
	{
		try
		{
			// [data, etag]
			var result = await ReadAsync((tx, dir) => tx.GetRangeValues(dir.EncodeRange(grainId.GetUniformHashCode(), reminderName)).ToListAsync());
			var entry = Deserialize<ReminderEntry>(result[0]);
			entry.ETag = ParseEtag(result[1]);
			return entry;
		}
		catch (Exception ex)
		{
			logger.LogError(ex, "Failure reading reminder {Name} for service {ServiceId} and grain {GrainId}",
				reminderName, _clusterOptions.ServiceId, grainId);
			WrappedException.CreateAndRethrow(ex);
			throw;
		}
	}

	public async Task<string> UpsertRow(ReminderEntry entry)
	{
		try
		{
			var db = await fdb.GetDatabase(new());
			VersionStamp stamp = await db.ReadWriteAsync(async tx =>
			{
				var dir = await GetDirectory(tx);
				var etagKey = dir.Encode(entry.GrainId.GetUniformHashCode(), entry.ReminderName, "etag");
				// var dbEtag = await tx.GetAsync(etagKey);
				// if (!dbEtag.IsNullOrEmpty && entry.ETag != dbEtag.ToUuid80().ToString())
				// {
				// 	throw new Exception(
				// 		$"Version conflict ({nameof(UpsertRow)}): ServiceId={_clusterOptions.ServiceId} ProviderName=FoundationDB GrainId={entry.GrainId} ETag={entry.ETag}.");
				// }

				tx.Set(dir.Encode(entry.GrainId.GetUniformHashCode(), entry.ReminderName, "data"), Serialize(entry));
				tx.SetVersionStampedValue(etagKey, tx.CreateVersionStamp().ToSlice());
				return new { stamp = tx.GetVersionStampAsync() };
			}, (_, res) => res.stamp, new());

			return ParseEtag(stamp.ToSlice())!;
		}
		catch (Exception ex)
		{
			logger.LogError("Failed to upsert reminder. state for GrainId={GrainId}, ReminderName={ReminderName}",
				entry.GrainId,
				entry.ReminderName);
			WrappedException.CreateAndRethrow(ex);
			throw;
		}
	}

	public Task<bool> RemoveRow(GrainId grainId, string reminderName, string etag)
	{
		try
		{
			return ReadWriteAsync(async (tx, dir) =>
			{
				var dbEtag = ParseEtag(await tx.GetAsync(dir.Encode(grainId.GetUniformHashCode(), reminderName, "etag")));

				if (dbEtag != etag)
					return false;

				tx.ClearRange(dir.EncodeRange(grainId.GetUniformHashCode(), reminderName));
				return true;
			});
		}
		catch (Exception ex)
		{
			logger.LogError("Failed to clear reminder. state for GrainId={GrainId}, ReminderName={ReminderName}.",
				grainId,
				reminderName);
			WrappedException.CreateAndRethrow(ex);
			throw;
		}
	}

	public Task TestOnlyClearTable()
	{
		return WriteAsync((tx, dir) => tx.ClearRange(dir));
	}

	T Deserialize<T>(Slice data) => JsonConvert.DeserializeObject<T>(data.ToStringUtf8()!, _jsonSettings)!;

	Slice Serialize<T>(T item) => Slice.FromStringUtf8(JsonConvert.SerializeObject(item, _jsonSettings));

	async ValueTask<FdbDirectorySubspace> GetDirectory(IFdbReadOnlyTransaction t)
	{
		var subspace = await fdb.Root[DirName].Resolve(t);
		return subspace!;
	}

	string? ParseEtag(Slice etag) => !etag.IsNullOrEmpty ? etag.ToUuid80().ToString() : null;

	Task WriteAsync(Func<IFdbTransaction, FdbDirectorySubspace, Task> write)
		=> fdb.WriteAsync(async tx => await write(tx, await GetDirectory(tx)), new());

	Task<T> ReadWriteAsync<T>(Func<IFdbTransaction, FdbDirectorySubspace, Task<T>> readWrite)
		=> fdb.ReadWriteAsync(async tx => await readWrite(tx, await GetDirectory(tx)), new());

	Task WriteAsync(Action<IFdbTransaction, FdbDirectorySubspace> write)
		=> fdb.WriteAsync(async tx => write(tx, await GetDirectory(tx)), new());

	Task<T> ReadAsync<T>(Func<IFdbReadOnlyTransaction, FdbDirectorySubspace, Task<T>> read)
		=> fdb.ReadAsync(async tx => await read(tx, await GetDirectory(tx)), new());
}