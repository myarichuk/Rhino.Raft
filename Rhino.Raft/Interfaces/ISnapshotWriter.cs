using System;
using System.IO;

namespace Rhino.Raft.Interfaces
{
	public interface ISnapshotWriter : IDisposable
	{
		long Index { get; 	}
		long Term { get; }
		void WriteSnapshot(Stream stream);
	}
}