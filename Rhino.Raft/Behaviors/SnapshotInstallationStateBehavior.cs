using System;
using System.IO;
using System.Threading.Tasks;
using Rhino.Raft.Messages;

namespace Rhino.Raft.Behaviors
{
	public class SnapshotInstallationStateBehavior : AbstractRaftStateBehavior
	{
		private readonly Random _random;

		private Task _installingSnapshot;

		public SnapshotInstallationStateBehavior(RaftEngine engine) : base(engine)
		{
			_random = new Random((int)(engine.Name.GetHashCode() + DateTime.UtcNow.Ticks));
			Timeout = _random.Next(engine.Options.MessageTimeout / 2, engine.Options.MessageTimeout);
		}

		public override RaftEngineState State
		{
			get { return RaftEngineState.SnapshotInstallation; }
		}

		public override CanInstallSnapshotResponse Handle(string destination, CanInstallSnapshotRequest req)
		{
			if (_installingSnapshot == null)
			{
				return base.Handle(destination, req);
			}
			return new CanInstallSnapshotResponse
			{
				From = Engine.Name,
				IsCurrentlyInstalling = true,
				Message = "The node is in the process of installing a snapshot",
				Success = false
			};
		}

		public override InstallSnapshotResponse Handle(MessageContext context, InstallSnapshotRequest req, Stream stream)
		{
			if (_installingSnapshot != null)
			{
				return base.Handle(context, req, stream);
			}

			var lastLogEntry = Engine.PersistentState.LastLogEntry();
			if (req.Term < lastLogEntry.Term)
			{
				stream.Dispose();

				return new InstallSnapshotResponse
				{
					From = Engine.Name,
					CurrentTerm = lastLogEntry.Term,
					LastLogIndex = lastLogEntry.Index,
					Message = "Term " + req.Term + " is older than last term in the log " + lastLogEntry.Term + " so the snapshot was rejected",
					Success = false
				};
			}
				
			Engine.DebugLog.Write("Received InstallSnapshotRequest from {0} until term {1} / {2}", req.From, req.LastIncludedTerm, req.LastIncludedIndex);

			Engine.OnSnapshotInstallationStarted();
			
			// this can be a long running task
			_installingSnapshot = Task.Run(() =>
			{
				try
				{
					Engine.StateMachine.ApplySnapshot(req.LastIncludedTerm, req.LastIncludedIndex, stream);
					Engine.PersistentState.MarkSnapshotFor(req.LastIncludedIndex, req.LastIncludedTerm, int.MaxValue);
				}
				catch (Exception e)
				{
					Engine.DebugLog.Write("Failed to install snapshot because {0}", e);
					context.ExecuteInEventLoop(() =>
					{
						_installingSnapshot = null;
					});
				}

				// we are doing it this way to ensure that we are single threaded
				context.ExecuteInEventLoop(() =>
				{
					Engine.UpdateCurrentTerm(req.Term, req.LeaderId);
					Engine.CommitIndex = req.LastIncludedIndex;
					Engine.DebugLog.Write("Updating the commit index to the snapshot last included index of {0}", req.LastIncludedIndex);
					Engine.OnSnapshotInstallationEnded(req.Term);

					context.Reply(new InstallSnapshotResponse
					{
						From = Engine.Name,
						CurrentTerm = lastLogEntry.Term,
						LastLogIndex = lastLogEntry.Index,
						Success = true
					});
				});
			});

			return null;
		}

		public override AppendEntriesResponse Handle(string destination, AppendEntriesRequest req)
		{
			var lastLogEntry = Engine.PersistentState.LastLogEntry();
			return
				new AppendEntriesResponse
				{
					From = Engine.Name,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					LastLogIndex = lastLogEntry.Index,
					LeaderId = Engine.CurrentLeader,
					Message = "I am in the process of receiving a snapshot, so I cannot accept new entries at the moment",
					Success = false
				};
		}


		public override void HandleTimeout()
		{
			Timeout = _random.Next(Engine.Options.MessageTimeout / 2, Engine.Options.MessageTimeout);
			LastHeartbeatTime = DateTime.UtcNow;// avoid busy loop while waiting for the snapshot
			Engine.DebugLog.Write("Received timeout during installation of a snapshot. Doing nothing, since the node should finish receiving snapshot before it could change into candidate");
			//do nothing during timeout --> this behavior will go on until the snapshot installation is finished
		}
	}
}
