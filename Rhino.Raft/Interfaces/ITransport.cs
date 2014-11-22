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
		bool TryReceiveMessage(int timeout, CancellationToken cancellationToken, out MessageContext messageContext);
	
		void Stream(string dest, InstallSnapshotRequest snapshotRequest, Action<Stream> streamWriter);
		
		void Send(string dest, CanInstallSnapshotRequest req);
		void Send(string dest, TimeoutNowRequest req);
		void Send(string dest, DisconnectedFromCluster req);
		void Send(string dest, AppendEntriesRequest req);
		void Send(string dest, RequestVoteRequest req);

		void SendToSelf(AppendEntriesResponse resp);
	}
}