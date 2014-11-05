using System; 
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using Rhino.Raft.Commands;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;
using Rhino.Raft.Storage;

namespace Rhino.Raft.Tests
{
	public class DictionaryStateMachine : IRaftStateMachine
	{
		private readonly JsonSerializer _serializer = new JsonSerializer();
		public long LastAppliedIndex { get; private set; }

		public int EntryCount
		{
			get { return Data.Count; }
		}

		private Dictionary<string, int> _snapshot;

		public Dictionary<string, int> Data = new Dictionary<string, int>();

		public void Apply(LogEntry entry, Command cmd)
		{
			if (LastAppliedIndex >= entry.Index)
				throw new InvalidOperationException("Already applied " + entry.Index);
			
			LastAppliedIndex = entry.Index;
			
			var dicCommand = cmd as DictionaryCommand;
			
			if (dicCommand != null) 
				dicCommand.Apply(Data);
		}

		public void CreateSnapshot()
		{
			_snapshot = new Dictionary<string, int>(Data);
		}

		public void WriteSnapshot(Stream stream)
		{
			if(_snapshot == null)
				throw new InvalidOperationException("There is no snapshot available");

			var streamWriter = new StreamWriter(stream);
			_serializer.Serialize(streamWriter, _snapshot);
			streamWriter.Flush();
		}

		public void ApplySnapshot(Stream stream)
		{
			Data = _serializer.Deserialize<Dictionary<string, int>>(new JsonTextReader(new StreamReader(stream)));
			_snapshot = new Dictionary<string, int>(Data);
		}
	}
}
