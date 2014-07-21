// -----------------------------------------------------------------------
//  <copyright file="RaftEngine.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using Consensus.Raft;
using Consensus.Raft.Behaviors;
using Rhino.Raft.Messages;
using Rhino.Raft.Storage;

namespace Rhino.Raft
{
	public class RaftEngine
	{
		public TextWriter DebugLog { get; set; }
		public ITransport Transport { get; set; }
		public TimeSpan HeartbeatTimeout { get; set; }
		public IRaftStateMachine StateMachine { get; set; }
		public IEnumerable<string> AllVotingPeers { get; set; }
		public IEnumerable<string> AllPeers { get; set; }
		public string Name { get; set; }
		public PersistentState PersistentState { get; set; }

		public string CurrentLeader { get; set; }
		public long CommitIndex { get; set; }
		public TimeSpan ElectionTimeout { get; set; }
		public int QuorumSize { get; set; }

		internal void UpdateCurrentTerm(long term)
		{
			PersistentState.UpdateTermTo(term);
			SetState(RaftEngineState.Follower);
			CurrentLeader = null;

		}

		internal void SetState(RaftEngineState raftEngineState)
		{
		}
		internal bool LogIsUpToDate(long lastLogTerm, long lastLogIndex)
		{
			// Raft paper 5.4.1
			var lastLogEntry = PersistentState.LastLogEntry();
			if (lastLogEntry.Term < lastLogTerm)
				return true;
			return lastLogEntry.Index <= lastLogIndex;
		}

		public void ApplyCommits(long from, long to)
		{
			foreach (LogEntry entry in PersistentState.LogEntriesAfter(from, to))
			{
				StateMachine.Apply(entry);
			}
			CommitIndex = to;
		}

		internal void AnnounceCandidacy()
		{
			PersistentState.IncrementTermAndVoteFor(Name);

			SetState(RaftEngineState.Candidate);

			DebugLog.WriteLine("Calling an election in term {0}", PersistentState.CurrentTerm);

			var lastLogEntry = PersistentState.LastLogEntry() ?? new LogEntry();
			var rvr = new RequestVoteRequest
			{
				CandidateId = Name,
				LastLogIndex = lastLogEntry.Index,
				LastLogTerm = lastLogEntry.Term,
				Term = PersistentState.CurrentTerm
			};

			foreach (var votingPeer in AllVotingPeers)
			{
				Transport.Send(votingPeer, rvr);
			}
		}

	}

	public interface IRaftStateMachine
	{
		long LastApplied { get; }
		void Apply(LogEntry entry);
	}

	public enum RaftEngineState
	{
		None,
		Follower,
		Leader,
		Candidate
	}
}