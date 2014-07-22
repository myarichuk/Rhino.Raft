using System;
using System.Diagnostics;
using System.Threading;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;


namespace Rhino.Raft.Behaviors
{
	public abstract class AbstractRaftStateBehavior : IHandler<RequestVoteRequest>, IHandler<AppendEntriesRequest>, IHandler<AppendEntriesResponse>
	{
		protected readonly RaftEngine Engine;
		protected readonly Stopwatch HeartbeatTimer;

		public string Name
		{
			get { return Engine.Name; }
		}

		public virtual void RunOnce()
		{
			var remaining = Engine.HeartbeatTimeout - HeartbeatTimer.Elapsed;
			if (remaining <= TimeSpan.Zero)
				HandleTimeout();

			Thread.Sleep(remaining);	
		}

		public event Action HeartbeatTimeout;

		protected virtual void OnTimeout()
		{
			var handler = HeartbeatTimeout;
			if (handler != null) handler();
		}

		protected AbstractRaftStateBehavior(RaftEngine engine)
		{
			Engine = engine;
			HeartbeatTimer = Stopwatch.StartNew();
			engine.Transport.RegisterHandler<RequestVoteRequest>(this);
			engine.Transport.RegisterHandler<AppendEntriesRequest>(this);
			engine.Transport.RegisterHandler<AppendEntriesResponse>(this);
		}

		public virtual void Handle(string source, AppendEntriesResponse resp)
		{
			HandleTimeout();
			// not a leader, no idea what to do with this. Probably an old
			// message from when we were a leader, ignoring.			
		}

		public virtual void Handle(string source, RequestVoteRequest req)
		{
			if (HandleTimeout())
				return;

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

		private bool HandleTimeout()
		{
			bool isTimeout = false;
			if (HeartbeatTimer.ElapsedMilliseconds > Engine.HeartbeatTimeout.TotalMilliseconds)
			{
				isTimeout = true;
				OnTimeout();
			}

			HeartbeatTimer.Reset();
			return isTimeout;
		}
		
		public virtual void Handle(string source, AppendEntriesRequest req)
		{
			if (HandleTimeout())
				return;
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

			if (req.LeaderCommit > Engine.CommitIndex)
			{
				long oldCommitIndex = Engine.CommitIndex;

				Engine.CommitIndex = Math.Min(req.LeaderCommit, req.Entries[req.Entries.Length - 1].Index);

				Engine.ApplyCommits(oldCommitIndex, Engine.CommitIndex);
			}

			Engine.Transport.Send(source, new AppendEntriesResponse
			{
				Success = true,
				CurrentTerm = Engine.PersistentState.CurrentTerm,
			});
		}		
	}
}