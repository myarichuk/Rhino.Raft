using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rhino.Raft.Commands;
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

		public void Apply(LogEntry entry, Command command)
		{
			if (LastApplied >= entry.Index)
				throw new InvalidOperationException("Already applied " + entry.Index);

			LastApplied = entry.Index;

			var dicCommand = (DictionaryCommand) command; 
			dicCommand.Apply(Data);
		}
	}
}
