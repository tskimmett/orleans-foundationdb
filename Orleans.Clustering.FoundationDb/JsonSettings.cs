using System;
using Newtonsoft.Json;
using System.Net;
using Orleans.Runtime;
using Newtonsoft.Json.Linq;
using System.Globalization;

namespace Orleans.Clustering.FoundationDb;


internal static class JsonSettings
{
	public static JsonSerializerSettings JsonSerializerSettings => new JsonSerializerSettings
	{
		Formatting = Formatting.None,
		TypeNameHandling = TypeNameHandling.None,
		DefaultValueHandling = DefaultValueHandling.Include,
		DateTimeZoneHandling = DateTimeZoneHandling.Utc,
		DateParseHandling = DateParseHandling.DateTimeOffset,
		Culture = CultureInfo.InvariantCulture,
		MissingMemberHandling = MissingMemberHandling.Ignore,
		PreserveReferencesHandling = PreserveReferencesHandling.None,
		MetadataPropertyHandling = MetadataPropertyHandling.Ignore,
		MaxDepth = 10,
		Converters = { new IpAddressConverter(), new IpEndPointConverter(), new SiloAddressConverter(), new TableVersionConverter() }
	};

	private class IpAddressConverter : JsonConverter
	{
		public override bool CanConvert(Type objectType)
		{
			return objectType == typeof(IPAddress);
		}

		public override void WriteJson(JsonWriter writer, object? value, JsonSerializer serializer)
		{
			IPAddress ip = (IPAddress)value!;
			writer.WriteValue(ip.ToString());
		}

		public override object ReadJson(JsonReader reader, Type objectType, object? existingValue, JsonSerializer serializer)
		{
			JToken token = JToken.Load(reader);
			return IPAddress.Parse(token.Value<string>()!);
		}
	}

	private class IpEndPointConverter : JsonConverter
	{
		public override bool CanConvert(Type objectType)
		{
			return objectType == typeof(IPEndPoint);
		}

		public override void WriteJson(JsonWriter writer, object? value, JsonSerializer serializer)
		{
			IPEndPoint ep = (IPEndPoint)value!;
			writer.WriteStartObject();
			writer.WritePropertyName("Address");
			serializer.Serialize(writer, ep.Address);
			writer.WritePropertyName("Port");
			writer.WriteValue(ep.Port);
			writer.WriteEndObject();
		}

		public override object ReadJson(JsonReader reader, Type objectType, object? existingValue, JsonSerializer serializer)
		{
			JObject jo = JObject.Load(reader);
			IPAddress address = jo["Address"]!.ToObject<IPAddress>(serializer)!;
			int port = jo["Port"]!.Value<int>();
			return new IPEndPoint(address, port);
		}
	}

	private class SiloAddressConverter : JsonConverter
	{
		public override bool CanConvert(Type objectType)
		{
			return objectType == typeof(SiloAddress);
		}

		public override void WriteJson(JsonWriter writer, object? value, JsonSerializer serializer)
		{
			SiloAddress addr = (SiloAddress)value!;
			writer.WriteStartObject();
			writer.WritePropertyName("SiloAddress");
			writer.WriteValue(addr.ToParsableString());
			writer.WriteEndObject();
		}

		public override object ReadJson(JsonReader reader, Type objectType, object? existingValue, JsonSerializer serializer)
		{
			JObject jo = JObject.Load(reader);
			SiloAddress addr = SiloAddress.FromParsableString(jo["SiloAddress"]!.ToObject<string>()!);
			return addr;
		}
	}

	private class TableVersionConverter : JsonConverter<TableVersion>
	{
		public override void WriteJson(JsonWriter writer, TableVersion? value, JsonSerializer serializer)
		{
			writer.WriteValue(value!.Version);
		}

		public override TableVersion? ReadJson(JsonReader reader, Type objectType, TableVersion? existingValue, bool hasExistingValue, JsonSerializer serializer)
		{
			if (reader.Value is long versionNumber)
				return new((int)versionNumber, versionNumber.ToString());
			return null;
		}
	}
}