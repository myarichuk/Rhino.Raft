using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Rhino.Raft.Messages;

namespace Rhino.Raft.Behaviors
{
	public class SnapshotInstallationStateBehavior : AbstractRaftStateBehavior
	{
		private readonly Random _random;

		public SnapshotInstallationStateBehavior(RaftEngine engine) : base(engine)
		{
			_random = new Random((int)(engine.Name.GetHashCode() + DateTime.UtcNow.Ticks));
			Timeout = _random.Next(engine.MessageTimeout / 2, engine.MessageTimeout);
		}

		public override RaftEngineState State
		{
			get { return RaftEngineState.SnapshotInstallation; }
		}

		public override void Handle(string destination, InstallSnapshotRequest req)
		{
			Engine.OnSnapshotInstallationStarted();
			base.Handle(destination, req);
			Engine.OnSnapshotInstallationEnded();
		}

		public override void Handle(string destination, AppendEntriesRequest req)
		{
			var lastLogEntry = Engine.PersistentState.LastLogEntry();
			Engine.Transport.Send(req.From,
				new AppendEntriesResponse
				{
					From = Engine.Name,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					LastLogIndex = lastLogEntry.Index,
					LeaderId = Engine.CurrentLeader,
					Message = "I am in the process of receiving a snapshot, so I cannot accept new entries at the moment",
					Source = destination,
					Success = false
				});
		}



		public override void HandleTimeout()
		{
			Timeout = _random.Next(Engine.MessageTimeout / 2, Engine.MessageTimeout);
			//do nothing during timeout --> this behavior will go on until the snapshot installation is finished
		}
	}
}
