using System; 
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
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

		private class SnapshotWriter : ISnapshotWriter
		{
			private readonly Dictionary<string, int> _snapshot;
			private readonly DictionaryStateMachine _parent;

			public SnapshotWriter(DictionaryStateMachine parent, Dictionary<string, int> snapshot)
			{
				_parent = parent;
				_snapshot = snapshot;
			}

			public void Dispose()
			{
			}

			public long Index { get; set; }
			public long Term { get; set; }
			public void WriteSnapshot(Stream stream)
			{
				var streamWriter = new StreamWriter(stream);
				_parent._serializer.Serialize(streamWriter, _snapshot);
				streamWriter.Flush();
				stream.Position = 0;
			}
		}

		public Dictionary<string, int> Data = new Dictionary<string, int>();
		private SnapshotWriter _snapshot;

		public void Apply(LogEntry entry, Command cmd)
		{
			if (LastAppliedIndex >= entry.Index)
				throw new InvalidOperationException("Already applied " + entry.Index);
			
			LastAppliedIndex = entry.Index;
			
			var dicCommand = cmd as DictionaryCommand;
			
			if (dicCommand != null) 
				dicCommand.Apply(Data);
		}

		public void CreateSnapshot(long index, long term)
		{
			_snapshot = new SnapshotWriter(this, new Dictionary<string, int>(Data))
			{
				Term = term,
				Index = index
			};
		}

		public ISnapshotWriter GetSnapshotWriter()
		{
			return _snapshot;
		}

		public void ApplySnapshot(long term, long index, Stream stream)
		{
			if(stream.CanSeek)
				stream.Position = 0;
			
			using (var streamReader = new StreamReader(stream))
				Data = _serializer.Deserialize<Dictionary<string, int>>(new JsonTextReader(streamReader));

			_snapshot = new SnapshotWriter(this, new Dictionary<string, int>(Data))
			{
				Term = term,
				Index = index
			};
		}
	}
}
