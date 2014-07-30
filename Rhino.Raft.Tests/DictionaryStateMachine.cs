using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;

namespace Rhino.Raft.Tests
{
	public class DictionaryStateMachine : IRaftStateMachine
	{
		private readonly JsonCommandSerializer _jsonCommandSerializer;
		public long LastApplied { get; private set; }

		public Dictionary<string, int> Data = new Dictionary<string, int>();

		public DictionaryStateMachine()
		{
			_jsonCommandSerializer = new JsonCommandSerializer();
		}

		public void Apply(LogEntry entry)
		{
			if (LastApplied >= entry.Index)
				throw new InvalidOperationException("Already applied " + entry.Index);

			LastApplied = entry.Index;

			var command = _jsonCommandSerializer.Deserialize(entry.Data) as DictionaryCommand; 
			if(command != null) //this means NopCommand -> it shouldn't be in the log
				command.Apply(Data);
		}
	}
}
