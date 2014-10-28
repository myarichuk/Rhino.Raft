using System;
using System.Diagnostics;
using System.Linq;
using Rhino.Raft.Commands;
using Rhino.Raft.Messages;
using Rhino.Raft.Storage;


namespace Rhino.Raft.Behaviors
{
	public abstract class AbstractRaftStateBehavior : IDisposable
	{
		protected readonly RaftEngine Engine;

		public string Name
		{
			get { return Engine.Name; }
		}


		public int Timeout { get; set; }
		public abstract RaftEngineState State { get; }

		public event Action<LogEntry[]> EntriesAppended;

		public virtual void HandleMessage(MessageEnvelope envelope)
		{
			RequestVoteRequest requestVoteRequest;
			RequestVoteResponse requestVoteResponse;
			AppendEntriesResponse appendEntriesResponse;
			AppendEntriesRequest appendEntriesRequest;

			if (TryCastMessage(envelope.Message, out requestVoteRequest))
				Handle(envelope.Destination, requestVoteRequest);
			else if (TryCastMessage(envelope.Message, out appendEntriesResponse))
				Handle(envelope.Destination, appendEntriesResponse);
			else if (TryCastMessage(envelope.Message, out appendEntriesRequest))
				Handle(envelope.Destination, appendEntriesRequest);
			else if (TryCastMessage(envelope.Message, out requestVoteResponse))
				Handle(envelope.Destination, requestVoteResponse);
		}

		public virtual void Handle(string destination, RequestVoteResponse resp)
		{

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
		}

		public void Handle(string destination, RequestVoteRequest req)
		{
			Engine.DebugLog.Write("Received RequestVoteRequest, req.CandidateId = {0}, term = {1}", req.CandidateId, req.Term);
			if (req.Term < Engine.PersistentState.CurrentTerm)
			{
				var msg = string.Format("Rejecting request vote because term {0} is lower than current term {1}",
					req.Term, Engine.PersistentState.CurrentTerm);
				Engine.DebugLog.Write(msg);
				Engine.Transport.Send(destination, new RequestVoteResponse
				{
					VoteGranted = false,
					Term = Engine.PersistentState.CurrentTerm,
					Message = msg,
					From = Engine.Name
				});
				return;
			}

			if (req.Term > Engine.PersistentState.CurrentTerm)
			{
				Engine.DebugLog.Write("{0} -> UpdateCurrentTerm() is called from Abstract Behavior", GetType().Name);
				Engine.UpdateCurrentTerm(req.Term, null);
			}

			if (Engine.PersistentState.VotedFor != null && Engine.PersistentState.VotedFor != req.CandidateId)
			{
				var msg = string.Format("Rejecting request vote because already voted for {0} in term {1}",
					Engine.PersistentState.VotedFor, req.Term);

				Engine.DebugLog.Write(msg);
				Engine.Transport.Send(destination, new RequestVoteResponse
				{
					VoteGranted = false,
					Term = Engine.PersistentState.CurrentTerm,
					Message = msg,
					From = Engine.Name
				});
				return;
			}

			if (Engine.LogIsUpToDate(req.LastLogTerm, req.LastLogIndex) == false)
			{
				var msg = string.Format("Rejecting request vote because remote log for {0} in not up to date.", req.CandidateId);
				Engine.DebugLog.Write(msg);
				Engine.Transport.Send(destination, new RequestVoteResponse
				{
					VoteGranted = false,
					Term = Engine.PersistentState.CurrentTerm,
					Message = msg,
					From = Engine.Name
				});
				return;
			}

			Engine.DebugLog.Write("Recording vote for candidate = {0}",req.CandidateId);
			Engine.PersistentState.RecordVoteFor(req.CandidateId);
			
			Engine.Transport.Send(req.CandidateId, new RequestVoteResponse
			{
				VoteGranted = true,
				Term = Engine.PersistentState.CurrentTerm,
				Message = "Vote granted",
				From = Engine.Name
			});
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
				var msg = string.Format("Rejecting append entries because msg term {0} is lower than current term {1}",
					req.Term, Engine.PersistentState.CurrentTerm);
				Engine.DebugLog.Write(msg);
				
				Engine.Transport.Send(req.LeaderId, new AppendEntriesResponse
				{
					Success = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					Message = msg,
					Source = Engine.Name
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
					Message = msg,
					LeaderId = req.LeaderId,
					Source = Engine.Name
				});
				return;
			}

			if (req.Entries.Length > 0)
			{
				Engine.DebugLog.Write("Appending log (persistant state), entries count: {0} (node state = {1})", req.Entries.Length,
					Engine.State);
				Engine.PersistentState.AppendToLog(req.Entries, req.PrevLogIndex);
				var topologyChange = req.Entries.LastOrDefault(x=>x.IsTopologyChange);
				
				// we consider the latest topology change to be in effect as soon as we see it, even before the 
				// it is committed, see raft spec section 6:
				//		a server always uses the latest conﬁguration in its log, 
				//		regardless of whether the entry is committed
				if (topologyChange != null) 
				{
					var command = Engine.PersistentState.CommandSerializer.Deserialize(topologyChange.Data);
					var topologyChangeCommand = command as TopologyChangeCommand;
					if(topologyChangeCommand == null)
						throw new ApplicationException("log entry that is marked with IsTopologyChange should be of type TopologyChangeCommand. Instead, it is of type: " + command.GetType());
					
					Engine.ChangingTopology = topologyChangeCommand.Requested;
					Engine.PersistentState.SetCurrentTopology(Engine.CurrentTopology, Engine.ChangingTopology);
				}
			}

			string message = String.Empty;

			var lastIndex = req.Entries.Length == 0 ?
				Engine.PersistentState.LastLogEntry().Index : 
				req.Entries[req.Entries.Length - 1].Index;
			try
			{
				if (req.LeaderCommit > Engine.CommitIndex)
				{
					Engine.DebugLog.Write(
						"preparing to apply commits: req.LeaderCommit: {0}, Engine.CommitIndex: {1}, lastIndex: {2}", req.LeaderCommit,
						Engine.CommitIndex, lastIndex);
					var oldCommitIndex = Engine.CommitIndex + 1;

					Engine.CommitIndex = Math.Min(req.LeaderCommit, lastIndex);
					Engine.ApplyCommits(oldCommitIndex, Engine.CommitIndex);
					message = "Applied commits, new CommitIndex is " + Engine.CommitIndex;
					Engine.DebugLog.Write(message);

					OnEntriesAppended(req.Entries);
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
					Message = "Failed to apply new entries. Reason: " + e.Message,
					Source = Engine.Name
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
					Source = Engine.Name
				});
			}
		}

		public virtual void Dispose()
		{
			
		}

		protected virtual void OnEntriesAppended(LogEntry[] logEntries)
		{
			var handler = EntriesAppended;
			if (handler != null) handler(logEntries);
		}

	}
}