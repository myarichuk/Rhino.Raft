using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Rhino.Raft
{
	public class SnapshotInfo
	{
		public long LastIndex { get; private set; }

		public long Timestamp { get; private set; }

		public byte[] StateMachineStateRaw { get; private set; }

		public SnapshotInfo(long lastIndex, IEnumerable<byte> stateMachineStateRaw)
		{
			LastIndex = lastIndex;
			StateMachineStateRaw = stateMachineStateRaw.ToArray();
			Timestamp = DateTime.UtcNow.Ticks;
		}

		public SnapshotInfo(long lastIndex, Stream stateMachineState)
		{
			LastIndex = lastIndex;

			StateMachineStateRaw = new byte[stateMachineState.Length];
			stateMachineState.Read(StateMachineStateRaw, 0, (int)stateMachineState.Length);

			Timestamp = DateTime.UtcNow.Ticks;
		}	
	}
}
