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
	public class CandidateStateBehavior : AbstractRaftStateBehavior, IHandler<RequestVoteResponse>
    {
		private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
		public int VotesForMyLeadership { get; set; }

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
					VoteGranted = true
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
			base.Handle(source,resp);
			if (resp.Term > Engine.PersistentState.CurrentTerm)
			{
				Engine.UpdateCurrentTerm(resp.Term);
				return;
			}

			Timeout -= (int)_stopwatch.ElapsedMilliseconds;
			_stopwatch.Restart();

			if (resp.VoteGranted)
			{
				VotesForMyLeadership += 1;
				if (VotesForMyLeadership < Engine.QuorumSize)
					return;
				Engine.DebugLog.WriteLine("{0} was selected as leader", Engine.Name);
				Engine.SetState(RaftEngineState.Leader);
			}
		}

    }
}