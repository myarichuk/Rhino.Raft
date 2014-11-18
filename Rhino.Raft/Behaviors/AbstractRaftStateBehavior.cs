using System;
using System.IO;
using System.Linq;
using Rhino.Raft.Commands;
using Rhino.Raft.Messages;


namespace Rhino.Raft.Behaviors
{
	public abstract class AbstractRaftStateBehavior : IDisposable
	{
		protected readonly RaftEngine Engine;
		public int Timeout { get; set; }
		public abstract RaftEngineState State { get; }

		public DateTime LastHeartbeatTime { get; set; }

		public void HandleMessage(MessageEnvelope envelope)
		{
			RequestVoteRequest requestVoteRequest;
			RequestVoteResponse requestVoteResponse;
			AppendEntriesResponse appendEntriesResponse;
			AppendEntriesRequest appendEntriesRequest;
			InstallSnapshotRequest installSnapshotRequest;
			CanInstallSnapshotRequest canInstallSnapshotRequest;
			CanInstallSnapshotResponse canInstallSnapshotResponse;
			Action action;

			if (TryCastMessage(envelope.Message, out requestVoteRequest))
				Handle(envelope.Destination, requestVoteRequest);
			else if (TryCastMessage(envelope.Message, out appendEntriesResponse))
				Handle(envelope.Destination, appendEntriesResponse);
			else if (TryCastMessage(envelope.Message, out appendEntriesRequest))
				Handle(envelope.Destination, appendEntriesRequest);
			else if (TryCastMessage(envelope.Message, out requestVoteResponse))
				Handle(envelope.Destination, requestVoteResponse);
			else if (TryCastMessage(envelope.Message, out canInstallSnapshotRequest))
				Handle(envelope.Destination, canInstallSnapshotRequest);
			else if (TryCastMessage(envelope.Message, out installSnapshotRequest))
				Handle(envelope.Destination, installSnapshotRequest, envelope.Stream);
			else if (TryCastMessage(envelope.Message, out canInstallSnapshotResponse))
				Handle(envelope.Destination, canInstallSnapshotResponse);
			else if (TryCastMessage(envelope.Message, out action))
				action();

			Engine.OnEventsProcessed();
		}

		public virtual void Handle(string destination, InstallSnapshotRequest req, Stream stream)
	    {
			stream.Dispose();
			//not relevant here, ignoring
	    }

		public virtual void Handle(string destination, RequestVoteResponse resp)
		{
			//do nothing, irrelevant here
		}

		protected static bool TryCastMessage<T>(object abstractMessage, out T typedMessage)
			where T : class
		{
			typedMessage = abstractMessage as T;
			return typedMessage != null;
		}

		public abstract void HandleTimeout();

		protected AbstractRaftStateBehavior(RaftEngine engine)
		{
			Engine = engine;
			LastHeartbeatTime = DateTime.UtcNow;

		}

		public void Handle(string destination, RequestVoteRequest req)
		{
			//disregard RequestVoteRequest if this node receives regular heartbeats and the leader is known
			// Raft paper section 6 (cluster membership changes), this apply only if we are a follower, because
			// candidate and leaders both generate their own heartbeat messages
			var timeSinceLastHeartbeat = (int) (DateTime.UtcNow - LastHeartbeatTime).TotalMilliseconds;
			
		    if (State == RaftEngineState.Follower && 
				(timeSinceLastHeartbeat < (Timeout/2)) && Engine.CurrentLeader != null)
			{
				Engine.DebugLog.Write("Received RequestVoteRequest from a node within election timeout while leader exists, rejecting");
				Engine.Transport.Send(req.From, new RequestVoteResponse
				{
					VoteGranted = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					VoteTerm = req.Term,
					Message = "I currently have a leader and I am receiving heartbeats within election timeout.",
					From = Engine.Name,
					TrialOnly = req.TrialOnly
				});
				return;
			}

			if (Engine.ContainedInAllVotingNodes(req.From) == false)
			{
				Engine.DebugLog.Write("Received RequestVoteRequest from a node that isn't a member in the cluster: {0}, rejecting", req.CandidateId);
				Engine.Transport.Send(req.From, new RequestVoteResponse
				{
					VoteGranted = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					VoteTerm = req.Term,
					Message = "You are not a memeber in my cluster, and cannot be a leader",
					From = Engine.Name,
					TrialOnly = req.TrialOnly
				});
				return;
			}

			Engine.DebugLog.Write("Received RequestVoteRequest, req.CandidateId = {0}, term = {1}", req.CandidateId, req.Term);
			
			if (req.Term < Engine.PersistentState.CurrentTerm)
			{
				var msg = string.Format("Rejecting request vote because term {0} is lower than current term {1}",
					req.Term, Engine.PersistentState.CurrentTerm);
				Engine.DebugLog.Write(msg);
				Engine.Transport.Send(req.From, new RequestVoteResponse
				{
					VoteGranted = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					VoteTerm = req.Term,
					Message = msg,
					From = Engine.Name,
					TrialOnly = req.TrialOnly
				});
				return;
			}

			if (req.Term > Engine.PersistentState.CurrentTerm && req.TrialOnly == false)
			{
				Engine.DebugLog.Write("{0} -> UpdateCurrentTerm() is called from Abstract Behavior", GetType().Name);
				Engine.UpdateCurrentTerm(req.Term, null);
			}

			if (Engine.PersistentState.VotedFor != null && Engine.PersistentState.VotedFor != req.CandidateId)
			{
				var msg = string.Format("Rejecting request vote because already voted for {0} in term {1}",
					Engine.PersistentState.VotedFor, req.Term);

				Engine.DebugLog.Write(msg);
				Engine.Transport.Send(req.From, new RequestVoteResponse
				{
					VoteGranted = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					VoteTerm = req.Term,
					Message = msg,
					From = Engine.Name,
					TrialOnly = req.TrialOnly
				});
				return;
			}

			if (Engine.LogIsUpToDate(req.LastLogTerm, req.LastLogIndex) == false)
			{
				var msg = string.Format("Rejecting request vote because remote log for {0} in not up to date.", req.CandidateId);
				Engine.DebugLog.Write(msg);
				Engine.Transport.Send(req.From, new RequestVoteResponse
				{
					VoteGranted = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					VoteTerm = req.Term,
					Message = msg,
					From = Engine.Name,
					TrialOnly = req.TrialOnly
				});
				return;
			}
			// we said we would be voting for this guy, so we can give it a full election timeout, 
			// by treating this as a heart beat. This means we won't be timing out ourselves and trying
			// to become the leader
			LastHeartbeatTime = DateTime.UtcNow;

			if (req.TrialOnly == false)
			{
				Engine.DebugLog.Write("Recording vote for candidate = {0}", req.CandidateId);
				Engine.PersistentState.RecordVoteFor(req.CandidateId);
			}
			else
			{
				Engine.DebugLog.Write("Voted for candidate = {0} in trial election for term {1}", req.CandidateId, req.Term);
			}
			Engine.Transport.Send(req.CandidateId, new RequestVoteResponse
			{
				VoteGranted = true,
				CurrentTerm = Engine.PersistentState.CurrentTerm,
				VoteTerm = req.Term,
				Message = "Vote granted",
				From = Engine.Name,
				TrialOnly = req.TrialOnly
			});
		}

		public virtual void Handle(string destination, CanInstallSnapshotResponse resp)
		{
			//irrelevant here, so doing nothing (used only in LeaderStateBehavior)
		}

		public virtual void Handle(string destination, CanInstallSnapshotRequest req)
		{
			var lastLogEntry = Engine.PersistentState.LastLogEntry();

			var index = lastLogEntry.Index;
			if (req.Term <= Engine.PersistentState.CurrentTerm && req.Index <= index)
			{
				Engine.Transport.Send(req.From, new CanInstallSnapshotResponse
				{
					From = Engine.Name,
					IsCurrentlyInstalling = false,
					Message = String.Format("Term or Index do not match the ones on this node. Cannot install snapshot. (CurrentTerm = {0}, req.Term = {1}, LastLogEntry index = {2}, req.Index = {3}",
						Engine.PersistentState.CurrentTerm,req.Term, index,req.Index),
					Success = false,
					Index = index,
					Term = Engine.PersistentState.CurrentTerm
				});
				return;
			}

			Engine.Transport.Send(req.From, new CanInstallSnapshotResponse
			{
				From = Engine.Name,
				IsCurrentlyInstalling = false,
				Message = "Everything ok, go ahead, install the snapshot!",
				Success = true
			});

			Engine.SetState(RaftEngineState.SnapshotInstallation);
		}

		public virtual void Handle(string destination, AppendEntriesResponse resp)
		{
			// not a leader, no idea what to do with this. Probably an old
			// message from when we were a leader, ignoring.			
		}
	
		public virtual void Handle(string destination, AppendEntriesRequest req)
		{
			if (req.Term < Engine.PersistentState.CurrentTerm)
			{
				var msg = string.Format(
					"Rejecting append entries because msg term {0} is lower then current term: {1}",
					req.Term, Engine.PersistentState.CurrentTerm);

				Engine.DebugLog.Write(msg);

				Engine.Transport.Send(req.LeaderId, new AppendEntriesResponse
				{
					Success = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					LastLogIndex = Engine.PersistentState.LastLogEntry().Index,
					LeaderId = Engine.CurrentLeader,
					Message = msg,
					From = Engine.Name,
				});
				return;
			}

			if (req.Term > Engine.PersistentState.CurrentTerm)
			{
				Engine.UpdateCurrentTerm(req.Term, req.LeaderId);
			}		

			if (Engine.CurrentLeader == null || req.LeaderId.Equals(Engine.CurrentLeader) == false)
			{
				Engine.CurrentLeader = req.LeaderId;
				Engine.DebugLog.Write("Set current leader to {0}", req.LeaderId);
			}

			var prevTerm = Engine.PersistentState.TermFor(req.PrevLogIndex) ?? 0;
			if (prevTerm != req.PrevLogTerm)
			{
				var msg = string.Format(
					"Rejecting append entries because msg previous term {0} is not the same as the persisted current term {1} at log index {2}",
					req.PrevLogTerm, prevTerm, req.PrevLogIndex);
				Engine.DebugLog.Write(msg);
				Engine.Transport.Send(req.LeaderId, new AppendEntriesResponse
				{
					Success = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					LastLogIndex = Engine.PersistentState.LastLogEntry().Index,
					Message = msg,
					LeaderId = req.LeaderId,
					From = Engine.Name
				});
				return;
			}

			LastHeartbeatTime = DateTime.UtcNow;
			if (req.Entries.Length > 0)
			{
				Engine.DebugLog.Write("Appending log (persistant state), entries count: {0} (node state = {1})", req.Entries.Length,
					Engine.State);
				Engine.PersistentState.AppendToLog(Engine, req.Entries, req.PrevLogIndex);
				var topologyChange = req.Entries.LastOrDefault(x=>x.IsTopologyChange == true);
				
				// we consider the latest topology change to be in effect as soon as we see it, even before the 
				// it is committed, see raft spec section 6:
				//		a server always uses the latest conﬁguration in its log, 
				//		regardless of whether the entry is committed
				if (topologyChange != null) 
				{
					var command = Engine.PersistentState.CommandSerializer.Deserialize(topologyChange.Data);
					var topologyChangeCommand = command as TopologyChangeCommand;
					if(topologyChangeCommand == null) //precaution,should never be true
													  //if this is true --> it is a serious issue and should be fixed immediately!
						throw new InvalidOperationException(@"Log entry that is marked with IsTopologyChange should be of type TopologyChangeCommand.
															Instead, it is of type: " + command.GetType() +". It is probably a bug!");

					Engine.DebugLog.Write("Topology change started (TopologyChangeCommand committed to the log): {0}", string.Join(", ", topologyChangeCommand.Requested));
					Engine.PersistentState.SetCurrentTopology(topologyChangeCommand.Requested, topologyChange.Index);
					Engine.TopologyChangeStarting(topologyChangeCommand);

					Engine.OnTopologyChangeStarted(topologyChangeCommand);
				}
			}

			var message = String.Empty;
			var lastIndex = req.Entries.Length == 0 ?
				Engine.PersistentState.LastLogEntry().Index : 
				req.Entries[req.Entries.Length - 1].Index;
			try
			{
				if (req.LeaderCommit > Engine.CommitIndex)
				{
					message = CommitEntries(req.Entries, lastIndex, req.LeaderCommit);
				}
				else
				{
					message = "Got new entries";
				}
			}
			catch (Exception e)
			{
				Engine.Transport.Send(req.LeaderId, new AppendEntriesResponse
				{
					Success = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					LastLogIndex = Engine.PersistentState.LastLogEntry().Index,
					Message = "Failed to apply new entries. Reason: " + e,
					From = Engine.Name
				});
				
			}
			finally
			{
				Engine.Transport.Send(req.LeaderId, new AppendEntriesResponse
				{
					Success = true,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					LastLogIndex = Engine.PersistentState.LastLogEntry().Index,
					Message = message,
					From = Engine.Name
				});				
			}
		}

		protected string CommitEntries(LogEntry[] entries, long lastIndex, long leaderCommit)
		{
			Engine.DebugLog.Write(
				"preparing to apply commits: req.LeaderCommit: {0}, Engine.CommitIndex: {1}, lastIndex: {2}", leaderCommit,
				Engine.CommitIndex, lastIndex);
			var oldCommitIndex = Engine.CommitIndex + 1;

			Engine.CommitIndex = Math.Min(leaderCommit, lastIndex);
			Engine.ApplyCommits(oldCommitIndex, Engine.CommitIndex);
			var message = "Applied commits, new CommitIndex is " + Engine.CommitIndex;
			Engine.DebugLog.Write(message);

			Engine.OnEntriesAppended(entries);
			return message;
		}

		public virtual void Dispose()
		{
			
		}
	}
}