using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rhino.Raft.Commands;

namespace Rhino.Raft.Tests
{
	public abstract class DictionaryCommand : Command
	{
		public abstract void Apply(ConcurrentDictionary<string, int> data);

		public string Key { get; set; }

		public class Set : DictionaryCommand
		{
			public int Value { get; set; }

			public override void Apply(ConcurrentDictionary<string, int> data)
			{
				data[Key] = Value;
			}
		}

		public class Inc : DictionaryCommand
		{
			public int Value { get; set; }

			public override void Apply(ConcurrentDictionary<string, int> data)
			{
				int value;
				data.TryGetValue(Key, out value);
				data[Key] = value + Value;
			}
		}


		public class Del : DictionaryCommand
		{
			public override void Apply(ConcurrentDictionary<string, int> data)
			{
				int value;
				data.TryRemove(Key, out value);
			}
		}
	}
}
