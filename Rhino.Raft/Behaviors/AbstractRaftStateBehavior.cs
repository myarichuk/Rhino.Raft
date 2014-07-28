using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;
using Voron;


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
			Engine.DebugLog.WriteLine("{0} -> Received RequestVoteRequest, req.CandidateId = {1}, term = {2}", Engine.Name,req.CandidateId, req.Term);
			if (req.Term < Engine.PersistentState.CurrentTerm)
			{
				var msg = string.Format("{0} -> Rejecting request vote because term {1} is lower than current term {2}",
					Engine.Name, req.Term, Engine.PersistentState.CurrentTerm);
				Engine.DebugLog.WriteLine(msg);
				Engine.Transport.Send(destination, new RequestVoteResponse
				{
					VoteGranted = false,
					Term = Engine.PersistentState.CurrentTerm,
					Message = msg
				});
				return;
			}

			if (req.Term > Engine.PersistentState.CurrentTerm)
			{
				Engine.UpdateCurrentTerm(req.Term);
			}

			if (Engine.PersistentState.VotedFor != null && Engine.PersistentState.VotedFor != req.CandidateId)
			{
				var msg = string.Format("{0} -> Rejecting request vote because already voted for {1} in term {2}",
					Engine.Name, Engine.PersistentState.VotedFor, req.Term);

				Engine.DebugLog.WriteLine(msg);
				Engine.Transport.Send(destination, new RequestVoteResponse
				{
					VoteGranted = false,
					Term = Engine.PersistentState.CurrentTerm,
					Message = msg
				});
				return;
			}

			if (Engine.LogIsUpToDate(req.LastLogTerm, req.LastLogIndex) == false)
			{
				var msg = string.Format("{0} -> Rejecting request vote because remote log for {1} in not up to date.", Engine.Name, req.CandidateId);
				Engine.DebugLog.WriteLine(msg);
				Engine.Transport.Send(destination, new RequestVoteResponse
				{
					VoteGranted = false,
					Term = Engine.PersistentState.CurrentTerm,
					Message = msg
				});
				return;
			}

			Engine.DebugLog.WriteLine("{0} -> Recording vote for candidate = {1}", Engine.Name,req.CandidateId);
			Engine.PersistentState.RecordVoteFor(req.CandidateId);

			Engine.Transport.Send(req.CandidateId, new RequestVoteResponse
			{
				VoteGranted = true,
				Term = Engine.PersistentState.CurrentTerm,
				Message = "Vote granted"
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
				var msg = string.Format("{0} -> Rejecting append entries because msg term {1} is lower than current term {2}",
					Engine.Name,req.Term, Engine.PersistentState.CurrentTerm);
				Engine.DebugLog.WriteLine(msg);
				Engine.Transport.Send(destination, new AppendEntriesResponse
				{
					Success = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					Message = msg
				});
				return;
			}
			if (req.Term > Engine.PersistentState.CurrentTerm)
			{
				Engine.UpdateCurrentTerm(req.Term);
			}

			Engine.CurrentLeader = req.LeaderId;
			var prevTerm = Engine.PersistentState.TermFor(req.PrevLogIndex) ?? 0;
			if (prevTerm != req.PrevLogTerm)
			{
				string msg = string.Format(
					"{0} Rejecting append entries because msg previous term {1} is not the same as the persisted current term {2} at log index {3}",
					Engine.Name, req.PrevLogTerm, prevTerm, req.PrevLogIndex);
				Engine.DebugLog.WriteLine(msg);
				Engine.Transport.Send(destination, new AppendEntriesResponse
				{
					Success = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					Message = msg,
				});
				return;
			}

			if (req.Entries.Length > 0)
			{
// ReSharper disable once CoVariantArrayConversion
				Engine.DebugLog.WriteLine("{0} ,Appending log, entries count: {1} (node state = {2})", Engine.Name, req.Entries.Length, Engine.State);
				Engine.PersistentState.AppendToLog(req.Entries, removeAllAfter: req.PrevLogIndex);
			}

			string message;
			long lastIndex;

			if (req.Entries.Length > 0)
			{
				lastIndex = req.Entries[req.Entries.Length - 1].Index;

				if (req.LeaderCommit > Engine.CommitIndex)
				{
					long oldCommitIndex = Engine.CommitIndex;

					Engine.CommitIndex = Math.Min(req.LeaderCommit, lastIndex);

					Engine.ApplyCommits(oldCommitIndex, Engine.CommitIndex);
				}
				message = "Applied commits";
			}
			else
			{
				message = "No commits to apply";
				lastIndex = Engine.CommitIndex;
			}

			Engine.Transport.Send(destination, new AppendEntriesResponse
			{
				Success = true,
				CurrentTerm = Engine.PersistentState.CurrentTerm,
				LastLogIndex = lastIndex,
				Message = message 
			});
		}

		public virtual void Dispose()
		{
			
		}
	}
}