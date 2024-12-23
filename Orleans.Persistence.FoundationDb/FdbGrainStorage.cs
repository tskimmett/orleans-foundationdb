﻿using System.IO.Compression;
using Doxense.Collections.Tuples;
using FoundationDB.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Persistence.FoundationDb;

public class FdbGrainStorage(
	string name,
	IFdbDatabaseProvider fdb,
	IGrainStorageSerializer grainStorageSerializer,
	IOptions<ClusterOptions> clusterOptions,
	ILogger<FdbGrainStorage> logger)
	: IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
{
	const string Brotli = "brotli";
	const string DirName = "orleans-grains";
	const int CompressionThreshold = 10_000;
	const int MaxValueBytes = 100_000;

	readonly ClusterOptions clusterOptions = clusterOptions.Value;

	public string ServiceId => clusterOptions.ServiceId;

	public void Participate(ISiloLifecycle lifecycle)
	{
		var name1 = OptionFormattingUtilities.Name<FdbGrainStorage>(name);
		lifecycle.Subscribe(name1, ServiceLifecycleStage.ApplicationServices, ct =>
		{
			// ensure grain state directory exists
			return fdb.WriteAsync(tx => fdb.Root[DirName].CreateOrOpenAsync(tx), ct);
		});
	}

	public async Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
	{
		try
		{
			await fdb.WriteAsync(async tx =>
			{
				var dir = await GetDir(tx);
				var subspace = GetGrainSubspace(dir, stateName, grainId);
				var storedEtag = ParseEtag(await tx.GetAsync(dir.Pack(subspace.Append("etag"))));

				if (storedEtag != grainState.ETag)
					throw new InconsistentStateException("Etag mismatch", storedEtag, grainState.ETag);

				tx.ClearRange(dir.PackRange(subspace));
			}, new());

			grainState.RecordExists = false;
			grainState.ETag = null;
			grainState.State = Activator.CreateInstance<T>();
		}
		catch (Exception ex) when (ex is not InconsistentStateException)
		{
			logger.LogError("Failed to clear grain state for {GrainType} grain with ID {GrainId}.", stateName, grainId);
			throw new FdbStorageException(
				$"Failed to clear grain state for {stateName} with ID {grainId}. {ex.GetType()}: {ex.Message}");
		}
	}

	public async Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
	{
		try
		{
			var (etag, data, encoding) = await fdb.ReadAsync(async tx =>
			{
				var dir = await GetDir(tx);
				var subspace = GetGrainSubspace(dir, stateName, grainId);
				var etag = tx.GetAsync(dir.Pack(subspace.Append("etag")));
				var data = tx.GetAsync(dir.Pack(subspace.Append("data")));
				var encoding = tx.GetValueStringAsync(dir.Pack(subspace.Append("encoding")));

				return (await etag, await data, await encoding);
			}, new());

			grainState.RecordExists = etag != Slice.Nil;
			if (grainState.RecordExists)
			{
				grainState.ETag = etag.ToUuid80().ToString();
				if (encoding is Brotli)
					data = await Decompress(data);
				grainState.State = grainStorageSerializer.Deserialize<T>(data.Memory);
			}
			else
			{
				grainState.ETag = null;
				grainState.State = Activator.CreateInstance<T>();
			}
		}
		catch (Exception ex)
		{
			logger.LogError("Failed to read grain state for {GrainType} grain with ID {GrainId}.", stateName, grainId);
			throw new FdbStorageException(
				$"Failed to read grain state for {stateName} with ID {grainId}. {ex.GetType()}: {ex.Message}");
		}
	}

	STuple<string, string> GetGrainSubspace(FdbDirectorySubspace dir, string stateName, GrainId grainId)
	{
		return STuple.Create(grainId.ToString(), stateName);
	}

	string? ParseEtag(Slice etag) => !etag.IsNullOrEmpty ? etag.ToUuid80().ToString() : null;

	public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
	{
		try
		{
			string? encoding = null;
			var data = grainStorageSerializer.Serialize(grainState.State);
			if (data.Length > CompressionThreshold)
			{
				var originalSize = data.Length;
				data = await Compress<T>(data);
				encoding = Brotli;
				if (data.Length >= MaxValueBytes)
				{
					throw new ArgumentOutOfRangeException("GrainState.Size",
						$"Value too large to write to FoundationDB. Size={data.Length} MaxSize={MaxValueBytes}");
				}

				logger.LogWarning("Compressed {State} state for grain {GrainId}. {Original}B -> {Compressed}B",
					stateName, grainId,
					originalSize, data.Length);
			}

			var db = await fdb.GetDatabase(new());
			VersionStamp stamp = await db.ReadWriteAsync(async tx =>
			{
				var dir = await GetDir(tx);
				var subspace = GetGrainSubspace(dir, stateName, grainId);
				var storedEtag = ParseEtag(await tx.GetAsync(dir.Pack(subspace.Append("etag"))));

				if (storedEtag != grainState.ETag)
					throw new InconsistentStateException(
						$"Version conflict ({nameof(WriteStateAsync)}): ServiceId={ServiceId} ProviderName={name} GrainType={stateName} GrainId={grainId} ETag={grainState.ETag}.");

				tx.SetVersionStampedValue(dir.Pack(subspace.Append("etag")), tx.CreateVersionStamp().ToSlice());
				tx.Set(dir.Pack(subspace.Append("data")), data);
				if (encoding is null)
					tx.Clear(dir.Pack(subspace.Append("encoding")));
				else
					tx.SetValueString(dir.Pack(subspace.Append("encoding")), Brotli);
				return new { stamp = tx.GetVersionStampAsync() };
			}, (_, res) => res.stamp, new());

			grainState.RecordExists = true;
			grainState.ETag = ParseEtag(stamp.ToSlice());
		}
		catch (Exception ex) when (ex is not InconsistentStateException)
		{
			logger.LogError("Failed to write grain state for {GrainType} grain with ID {GrainId}.", stateName, grainId);
			throw new FdbStorageException(
				$"Failed to write grain state for {stateName} with ID {grainId}. {ex.GetType()}: {ex.Message}");
		}
	}

	static async Task<BinaryData> Compress<T>(BinaryData data)
	{
		await using var compressed = new MemoryStream();
		await using var brotli = new BrotliStream(compressed, CompressionLevel.Fastest);
		await data.ToStream().CopyToAsync(brotli);
		compressed.Position = 0;
		return await BinaryData.FromStreamAsync(compressed);
	}

	async Task<Slice> Decompress(Slice data)
	{
		await using var brotli = new BrotliStream(data.ToStream(), CompressionMode.Decompress);
		return await Slice.FromStreamAsync(brotli, new());
	}

	async Task<FdbDirectorySubspace> GetDir(IFdbReadOnlyTransaction tx)
	{
		return await fdb.Root[DirName].Resolve(tx) ?? throw new Exception($"{DirName} directory does not exist.");
	}
}