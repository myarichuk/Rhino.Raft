// -----------------------------------------------------------------------
//  <copyright file="CandidateStateBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Data;
using System.Diagnostics;
using System.Threading;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;

namespace Rhino.Raft.Behaviors
{
	public class CandidateStateBehavior : AbstractRaftStateBehavior
    {
		private int _votesForMyLeadership;
		private readonly Random _random;
		public int VotesForMyLeadership
		{
			get { return _votesForMyLeadership; }
			set { _votesForMyLeadership = value; }
		}

		public CandidateStateBehavior(RaftEngine engine) : base(engine)
		{
			_random = new Random(engine.Name.GetHashCode());
			Timeout = _random.Next(engine.MessageTimeout / 2, engine.MessageTimeout);
			VoteForSelf();
	    }

		private void VoteForSelf()
		{
			Engine.DebugLog.Write("Voting for myself in term {0}", Engine.PersistentState.CurrentTerm);
			Handle(Engine.Name,
				new RequestVoteResponse
				{
					Term = Engine.PersistentState.CurrentTerm,
					VoteGranted = true,
					Message = String.Format("{0} -> Voting for myself", Engine.Name)
				});
		}

		public override void HandleTimeout()
	    {
			Engine.DebugLog.Write("Timeout ({1:#,#;;0} ms) for elections in term {0}", Engine.PersistentState.CurrentTerm,
				  Timeout);

			Timeout = _random.Next(Engine.MessageTimeout / 2, Engine.MessageTimeout); 
			VotesForMyLeadership = 0;
			Engine.AnnounceCandidacy();
			VoteForSelf();
	    }	

		public override void Handle(string source,RequestVoteResponse resp)
		{
			if (resp.Term > Engine.PersistentState.CurrentTerm)
			{
				Engine.DebugLog.Write("CandidateStateBehavior -> UpdateCurrentTerm called");
				Engine.UpdateCurrentTerm(resp.Term, null);
				return;
			}

			if (!resp.VoteGranted)
			{
				return;
			}

			Interlocked.Increment(ref _votesForMyLeadership);
			if (_votesForMyLeadership < Engine.QuorumSize)
			{
				Engine.DebugLog.Write("Not enough votes for leadership, vote count = {0}",_votesForMyLeadership);
				return;
			}

			Engine.DebugLog.Write("Selected as leader 1, term = {0}", resp.Term);
			Engine.SetState(RaftEngineState.Leader);
		}

    }
}