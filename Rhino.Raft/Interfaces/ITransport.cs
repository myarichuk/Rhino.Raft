using System;
using System.IO;
using System.Threading;
using Rhino.Raft.Messages;

namespace Rhino.Raft.Interfaces
{

	/// <summary>
	/// abstraction for transport between Raft nodes.
	/// </summary>
	public interface ITransport
	{
		bool TryReceiveMessage(string dest, int timeout, CancellationToken cancellationToken, out MessageEnvelope messageEnvelope);
	
		void Stream(string dest, InstallSnapshotRequest snapshotRequest, Action<Stream> streamWriter);
		void Send(string dest, CanInstallSnapshotRequest req);
		void Send(string dest, CanInstallSnapshotResponse resp);
		void Send(string dest, InstallSnapshotResponse resp);
		void Send(string dest, TimeoutNowRequest req);
		void Send(string dest, AppendEntriesRequest req);
		void Send(string dest, RequestVoteRequest req);
		void Send(string dest, AppendEntriesResponse resp);
		void Send(string dest, RequestVoteResponse resp);
		void Execute(string dest, Action action);
	}
}