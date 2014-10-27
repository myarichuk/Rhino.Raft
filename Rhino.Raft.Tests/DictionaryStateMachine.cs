using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Rhino.Raft.Commands;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;

namespace Rhino.Raft.Tests
{
	public class DictionaryStateMachine : IRaftStateMachine
	{
		private readonly JsonSerializer _serializer = new JsonSerializer();

		public long LastApplied { get; private set; }

		public long LastSnapshotIndex { get; private set; }

		public Dictionary<string, int> Data = new Dictionary<string, int>();

		public Dictionary<int,byte[]> Snapshots = new Dictionary<int, byte[]>(); 

		public void Apply(LogEntry entry, Command command)
		{
			if (LastApplied >= entry.Index)
				throw new InvalidOperationException("Already applied " + entry.Index);
			
			LastApplied = entry.Index;
			
			var dicCommand = command as DictionaryCommand;

			if (dicCommand != null) 
				dicCommand.Apply(Data);
		}

		public async Task CreateSnapshotAsync()
		{			
			var currentData = Data.Where(kvp => kvp.Value <= LastApplied)
								  .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
			var currentLastApplied = currentData.Max(x => x.Value);
			LastSnapshotIndex = currentLastApplied;

			var snapshotData = await Serialize(currentData);
			Snapshots.Add(currentLastApplied,snapshotData);
		}

		public Stream ReadSnapshot(int snapshotCutoffIndexId)
		{
			throw new NotImplementedException();
		}

		public Task WriteSnapshotAsync(Stream stream)
		{
			throw new NotImplementedException();
		}

		public async Task<byte[]> Serialize(Dictionary<string, int> data)
		{
			using (var memoryStream = new MemoryStream())
			{
				using (var streamWriter = new StreamWriter(memoryStream))
				{
					_serializer.Serialize(streamWriter, data);
					await streamWriter.FlushAsync();
				}
				return memoryStream.ToArray();
			}
		}
	}
}
