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
				Handle(envelope.Source, requestVoteRequest);
			else if (TryCastMessage(envelope.Message, out appendEntriesResponse))
				Handle(envelope.Source, appendEntriesResponse);
			else if (TryCastMessage(envelope.Message, out appendEntriesRequest))
				Handle(envelope.Source, appendEntriesRequest);
			else if (TryCastMessage(envelope.Message, out requestVoteResponse))
				Handle(envelope.Source, requestVoteResponse);
		}

		public virtual void Handle(string source, RequestVoteResponse resp)
		{
			Engine.AllVotingPeers = Engine.AllVotingPeers.Concat(new[] {source});
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

		public void Handle(string source, RequestVoteRequest req)
		{
			if (req.Term < Engine.PersistentState.CurrentTerm)
			{
				string msg = string.Format("Rejecting request vote because term {0} is lower than current term {1}",
					req.Term, Engine.PersistentState.CurrentTerm);
				Engine.DebugLog.WriteLine(msg);
				Engine.Transport.Send(source, new RequestVoteResponse
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
				string msg = string.Format("Rejecting request vote because already voted for {0} in term {1}",
					Engine.PersistentState.VotedFor, req.Term);
				Engine.DebugLog.WriteLine(msg);
				Engine.Transport.Send(source, new RequestVoteResponse
				{
					VoteGranted = false,
					Term = Engine.PersistentState.CurrentTerm,
					Message = msg
				});
				return;
			}

			if (Engine.LogIsUpToDate(req.LastLogTerm, req.LastLogIndex) == false)
			{
				string msg = string.Format("Rejecting request vote because remote log for {0} in not up to date.",
					req.CandidateId);
				Engine.DebugLog.WriteLine(msg);
				Engine.Transport.Send(source, new RequestVoteResponse
				{
					VoteGranted = false,
					Term = Engine.PersistentState.CurrentTerm,
					Message = msg
				});
				return;
			}
			Engine.PersistentState.RecordVoteFor(req.CandidateId);

			Engine.Transport.Send(source, new RequestVoteResponse
			{
				VoteGranted = true,
				Term = Engine.PersistentState.CurrentTerm,
				Message = "Vote granted"
			});
		}

		public virtual void Handle(string source, AppendEntriesResponse resp)
		{
			// not a leader, no idea what to do with this. Probably an old
			// message from when we were a leader, ignoring.			
		}
	
		public virtual void Handle(string source, AppendEntriesRequest req)
		{
			if (req.Term < Engine.PersistentState.CurrentTerm)
			{
				string msg = string.Format("Rejecting append entries because msg term {0} is lower than current term {1}",
					req.Term, Engine.PersistentState.CurrentTerm);
				Engine.DebugLog.WriteLine(msg);
				Engine.Transport.Send(source, new AppendEntriesResponse
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
			long prevTerm = Engine.PersistentState.TermFor(req.PrevLogIndex) ?? 0;
			if (prevTerm != req.PrevLogTerm)
			{
				string msg = string.Format(
					"Rejecting append entries because msg previous term {0} is not the same as the persisted current term {1} at log index {2}",
					req.PrevLogTerm, prevTerm, req.PrevLogIndex);
				Engine.DebugLog.WriteLine(msg);
				Engine.Transport.Send(source, new AppendEntriesResponse
				{
					Success = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					Message = msg,
				});
				return;
			}

			if (req.Entries.Length > 0)
			{
				Engine.PersistentState.AppendToLog(req.Entries, removeAllAfter: req.PrevLogIndex);
			}

			var lastIndex = req.Entries[req.Entries.Length - 1].Index;

			if (req.LeaderCommit > Engine.CommitIndex)
			{
				long oldCommitIndex = Engine.CommitIndex;

				Engine.CommitIndex = Math.Min(req.LeaderCommit, lastIndex);

				Engine.ApplyCommits(oldCommitIndex, Engine.CommitIndex);
			}

			Engine.Transport.Send(source, new AppendEntriesResponse
			{
				Success = true,
				CurrentTerm = Engine.PersistentState.CurrentTerm,
				LastLogIndex = lastIndex
			});
		}

		public virtual void Dispose()
		{
			
		}
	}
}