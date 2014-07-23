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
		private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
		private int _votesForMyLeadership;

		public int VotesForMyLeadership
		{
			get { return _votesForMyLeadership; }
			set { _votesForMyLeadership = value; }
		}

		public CandidateStateBehavior(RaftEngine engine) : base(engine)
		{
			Timeout = new Random().Next(engine.MessageTimeout/2, engine.MessageTimeout);
			VoteForSelf();
	    }

		private void VoteForSelf()
		{
			Engine.DebugLog.WriteLine("Voting for myself {0}", Engine.Name);
			Engine.Transport.Send(Engine.Name,
				new RequestVoteResponse
				{
					Term = Engine.PersistentState.CurrentTerm,
					VoteGranted = true,
					Message = String.Format("Voting for myself, node = {0}", Engine.Name)
				});
		}

		public override void HandleTimeout()
	    {
			Engine.DebugLog.WriteLine("Timeout for elections in term {0} for {1}", Engine.PersistentState.CurrentTerm,
				  Engine.Name);

			VotesForMyLeadership = 0;
			Engine.AnnounceCandidacy();
			VoteForSelf();
	    }

		public override void Handle(string source,RequestVoteResponse resp)
		{
			Engine.DebugLog.WriteLine("Received RequestVoteResponse on node = {2}, Term = {0}, VoteGranted = {1}, Message = {3}", resp.Term,resp.VoteGranted,Engine.Name,resp.Message);

			if (resp.Term > Engine.PersistentState.CurrentTerm)
			{
				Engine.UpdateCurrentTerm(resp.Term);
				return;
			}

			Timeout -= (int)_stopwatch.ElapsedMilliseconds;
			if (Timeout <= 0)
			{
				Timeout = 0;
				return;
			}

			_stopwatch.Restart();

			if (!resp.VoteGranted)
			{
				return;
			}

			Interlocked.Increment(ref _votesForMyLeadership);
			if (_votesForMyLeadership < Engine.QuorumSize)
				return;

			Engine.DebugLog.WriteLine("{0} was selected as leader, term = {1}", Engine.Name, resp.Term + 1);
			Engine.SetState(RaftEngineState.Leader);
		}

    }
}