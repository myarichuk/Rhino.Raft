using System.IO;
using Newtonsoft.Json;
using Rhino.Raft.Commands;

namespace Rhino.Raft
{
	public class JsonCommandSerializer : ICommandSerializer
	{
		private readonly JsonSerializer _serializer;

		public JsonCommandSerializer()
		{
			_serializer = new JsonSerializer
			{
				TypeNameHandling = TypeNameHandling.Objects
			};
		}

		public byte[] Serialize(Command cmd)
		{
			var memoryStream = new MemoryStream();
			var streamWriter = new StreamWriter(memoryStream);
			_serializer.Serialize(streamWriter, cmd);
			streamWriter.Flush();
			return memoryStream.ToArray();
		}

		public Command Deserialize(byte[] cmd)
		{
			return (Command)_serializer.Deserialize(new JsonTextReader(new StreamReader(new MemoryStream(cmd))));
		}
	}
}
