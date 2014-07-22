// -----------------------------------------------------------------------
//  <copyright file="CandidateStateBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Data;
using System.Threading;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;

namespace Rhino.Raft.Behaviors
{
	public class CandidateStateBehavior : AbstractRaftStateBehavior, IHandler<RequestVoteResponse>
    {
	    private readonly Random _random = new Random();
		private readonly TimeSpan _timeout;

		public CandidateStateBehavior(RaftEngine engine) : base(engine)
	    {
			Engine.Transport.RegisterHandler<RequestVoteResponse>(this);
		    _timeout = Engine.ElectionTimeout;
			VoteForSelf();
	    }

		public override void RunOnce()
		{
			var remaining = _timeout - HeartbeatTimer.Elapsed;
			if(remaining <= TimeSpan.Zero)
				HandleTimeout();

			Thread.Sleep(remaining);
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

	    public void HandleTimeout()
	    {
			Engine.DebugLog.WriteLine("Timeout for elections in term {0} for {1}", Engine.PersistentState.CurrentTerm,
				  Engine.Name);
		    var timeInMs = _random.Next(Engine.ElectionTimeout.Milliseconds / 6,
			    Engine.ElectionTimeout.Milliseconds / 2);

			Engine.Transport.HeartbeatTimeout = TimeSpan.FromMilliseconds(timeInMs);			

			VotesForMyLeadership = 0;
			Engine.AnnounceCandidacy();
			VoteForSelf();
	    }

		public void Handle(string source,RequestVoteResponse resp)
		{
			if (resp.Term > Engine.PersistentState.CurrentTerm)
			{
				Engine.UpdateCurrentTerm(resp.Term);
				return;
			}

			if (resp.VoteGranted)
			{
				VotesForMyLeadership += 1;
				if (VotesForMyLeadership < Engine.QuorumSize)
					return;
				Engine.DebugLog.WriteLine("{0} was selected as leader", Engine.Name);
				Engine.SetState(RaftEngineState.Leader);
			}
		}

	    public int VotesForMyLeadership { get; set; }
    }
}