using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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

		public long LastAppliedIndex { get; private set; }

		public long LastSnapshotIndex { get; private set; }

		public readonly Dictionary<string, int> Data = new Dictionary<string, int>();

		public readonly List<SnapshotInfo> Snapshots = new List<SnapshotInfo>(); 

		public void Apply(LogEntry entry, Command cmd)
		{
			if (LastAppliedIndex >= entry.Index)
				throw new InvalidOperationException("Already applied " + entry.Index);
			
			LastAppliedIndex = entry.Index;
			
			var dicCommand = cmd as DictionaryCommand;
			
			if (dicCommand != null) 
				dicCommand.Apply(Data);
		}

		public async Task CreateSnapshotAsync()
		{			
			var currentData = Data.Where(kvp => kvp.Value <= LastAppliedIndex)
								  .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
			var currentLastApplied = currentData.Max(x => x.Value);
			LastSnapshotIndex = currentLastApplied;

			var snapshotData = await Serialize(currentData);
			Snapshots.Add(new SnapshotInfo(LastSnapshotIndex,snapshotData));
		}

		public Stream ReadSnapshot(int snapshotIndexId)
		{
			var relevantSnapshotInfo = Snapshots.OrderBy(s => s.Timestamp)
												.LastOrDefault(s => s.LastIndex <= snapshotIndexId);
			if (relevantSnapshotInfo == null)
				return Stream.Null;

			return new MemoryStream(relevantSnapshotInfo.StateMachineStateRaw);
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
